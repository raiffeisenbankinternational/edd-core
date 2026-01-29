(ns edd.parquet.core
  "Parquet file generation for columnar data export.

   Schema format:
   {:description \"Table description\"
    :columns [[\"COL_NAME\" :type \"description\" :required/:optional & opts]
              ...]}

   Supported column types:
   - :string   - UTF-8 string (BINARY with STRING logical type)
   - :enum     - String with optional validation (BINARY with STRING logical type)
   - :boolean  - Stored as string \"TRUE\"/\"FALSE\" (BINARY with STRING logical type)
   - :uuid     - UUID as string (BINARY with STRING logical type)
   - :date     - Date as days since epoch (INT32 with DATE logical type)
   - :double   - 64-bit floating point (DOUBLE)
   - :long     - 64-bit signed integer (INT64)

   Example:
   (write-parquet-bytes
     {:table-name \"orders\"
      :schema {:description \"Customer orders\"
               :columns [[\"ORDER_ID\" :uuid \"Order identifier\" :required]
                         [\"CUSTOMER\" :string \"Customer name\" :required]
                         [\"AMOUNT\" :double \"Order amount\" :required]
                         [\"STATUS\" :enum \"Order status\" :required :enum [\"PENDING\" \"SHIPPED\" \"DELIVERED\"]]
                         [\"CREATED\" :date \"Creation date\" :optional]]}
      :rows [{\"ORDER_ID\" \"123e4567-e89b-12d3-a456-426614174000\"
              \"CUSTOMER\" \"Acme Corp\"
              \"AMOUNT\" 1234.56
              \"STATUS\" \"PENDING\"
              \"CREATED\" \"2024-01-15\"}]
      :compression :gzip})"
  (:require [clojure.tools.logging :as log])
  (:import [java.io ByteArrayOutputStream]
           [java.security MessageDigest]
           [java.util Base64]
           [org.apache.hadoop.conf Configuration]
           [org.apache.parquet.hadoop.api WriteSupport
            WriteSupport$WriteContext]
           [org.apache.parquet.hadoop.metadata CompressionCodecName]
           [org.apache.parquet.io OutputFile PositionOutputStream]
           [org.apache.parquet.io.api Binary]
           [org.apache.parquet.schema LogicalTypeAnnotation MessageType
            PrimitiveType$PrimitiveTypeName Type$Repetition Types]))

(set! *warn-on-reflection* true)

(def ^:private parquet-mime-type "application/vnd.apache.parquet")

(defn mime-type "Returns the MIME type for Parquet files." [] parquet-mime-type)

(defn- build-parquet-schema
  [table-name schema]
  (let [fields
        (mapv (fn [[col-name col-type _ requirement & _rest]]
                (let [repetition
                      (if (= requirement :required)
                        Type$Repetition/REQUIRED
                        Type$Repetition/OPTIONAL)]
                  (case col-type
                    :string (-> (Types/primitive
                                 PrimitiveType$PrimitiveTypeName/BINARY
                                 repetition)
                                (.as (LogicalTypeAnnotation/stringType))
                                (.named col-name))
                    :enum (-> (Types/primitive
                               PrimitiveType$PrimitiveTypeName/BINARY
                               repetition)
                              (.as (LogicalTypeAnnotation/stringType))
                              (.named col-name))
                    :boolean (-> (Types/primitive
                                  PrimitiveType$PrimitiveTypeName/BINARY
                                  repetition)
                                 (.as (LogicalTypeAnnotation/stringType))
                                 (.named col-name))
                    :uuid (-> (Types/primitive
                               PrimitiveType$PrimitiveTypeName/BINARY
                               repetition)
                              (.as (LogicalTypeAnnotation/stringType))
                              (.named col-name))
                    :date (-> (Types/primitive
                               PrimitiveType$PrimitiveTypeName/INT32
                               repetition)
                              (.as (LogicalTypeAnnotation/dateType))
                              (.named col-name))
                    :double (-> (Types/primitive
                                 PrimitiveType$PrimitiveTypeName/DOUBLE
                                 repetition)
                                (.named col-name))
                    :long (-> (Types/primitive
                               PrimitiveType$PrimitiveTypeName/INT64
                               repetition)
                              (.named col-name))
                    (-> (Types/primitive PrimitiveType$PrimitiveTypeName/BINARY
                                         repetition)
                        (.as (LogicalTypeAnnotation/stringType))
                        (.named col-name)))))
              (:columns schema))]
    (MessageType. ^String table-name ^java.util.List (vec fields))))

(defn- date-string->days-since-epoch
  "Convert YYYY-MM-DD date string to days since Unix epoch (1970-01-01)."
  [^String date-str]
  (let [date
        (java.time.LocalDate/parse date-str)]
    (.toEpochDay date)))

(defn- create-write-support
  [schema column-defs]
  (let [consumer
        (atom nil)]
    (proxy [WriteSupport] []
      (init [_configuration] (WriteSupport$WriteContext. schema {}))
      (prepareForWrite [record-consumer] (reset! consumer record-consumer))
      (write [record]
        (let [^org.apache.parquet.io.api.RecordConsumer rc
              @consumer]
          (.startMessage rc)
          (doseq [[idx [col-name col-type _ requirement & opts]]
                  (map-indexed vector column-defs)]
            (let [value
                  (get record col-name)

                  string-value
                  (when (some? value) (str value))

                  allowed-enum
                  (:enum (apply hash-map opts))]
              (if (some? value)
                (do
                  (.startField rc ^String col-name (int idx))
                  (case col-type
                    :string (.addBinary rc (Binary/fromString string-value))
                    :enum
                    (do
                      (when (and (some? allowed-enum)
                                 (not (some #{string-value} allowed-enum)))
                        (throw
                         (ex-info
                          (format
                           "Invalid enum value '%s' for column '%s' (allowed: %s)"
                           string-value
                           col-name
                           allowed-enum)
                          {:column col-name,
                           :value string-value,
                           :allowed allowed-enum})))
                      (.addBinary rc (Binary/fromString string-value)))
                    :boolean (.addBinary rc (Binary/fromString string-value))
                    :uuid (.addBinary rc (Binary/fromString string-value))
                    :date (.addInteger rc
                                       (int (date-string->days-since-epoch
                                             string-value)))
                    :double (.addDouble rc (double value))
                    :long (.addLong rc (long value))
                    (.addBinary rc (Binary/fromString string-value)))
                  (.endField rc ^String col-name (int idx)))
                (when (= requirement :required)
                  (throw (ex-info (format "Required column '%s' has nil value"
                                          col-name)
                                  {}))))))
          (.endMessage rc))))))

(defn- create-in-memory-output-file
  "Creates an in-memory OutputFile implementation that writes to a ByteArrayOutputStream."
  [^ByteArrayOutputStream baos]
  (reify
    OutputFile
    (create [_ _block-size-hint]
      (let [pos
            (long-array 1)]
        (proxy [PositionOutputStream] []
          (write
            ([b]
             (if (bytes? b)
               (let [b
                     (bytes b)]
                 (.write baos b 0 (alength b))
                 (aset pos 0 (unchecked-add (aget pos 0) (long (alength b)))))
               (do (.write baos (int b))
                   (aset pos 0 (unchecked-inc (aget pos 0))))))
            ([^bytes b off len]
             (.write baos b (int off) (int len))
             (aset pos 0 (unchecked-add (aget pos 0) (long len)))))
          (getPos [] (aget pos 0))
          (close [] (.close baos)))))
    (createOrOverwrite [this block-size-hint] (.create this block-size-hint))
    (supportsBlockSize [_] true)
    (defaultBlockSize [_] (* 128 1024 1024))))

(defn- filter-row-to-schema
  "Filter a row to only include columns defined in the table schema."
  [row schema-def]
  (let [schema-columns
        (set (map first (:columns schema-def)))]
    (select-keys row schema-columns)))

(defn schema-fingerprint
  "Returns a deterministic fingerprint for the schema definition.

   Intended to change when columns/types/requirements/enums change.
   Useful for cache invalidation or versioning."
  [schema]
  (let [normalized-columns
        (mapv (fn [[col-name col-type _description requirement & opts]]
                (let [opts-map
                      (apply hash-map opts)

                      enum-values
                      (if (= col-type :boolean)
                        ["TRUE" "FALSE"]
                        (:enum opts-map))]
                  {:name col-name,
                   :type col-type,
                   :requirement requirement,
                   :enum (when enum-values (vec enum-values))}))
              (:columns schema))

        payload
        (pr-str {:description (:description schema),
                 :columns normalized-columns})

        md
        (doto (MessageDigest/getInstance "SHA-256")
          (.update (.getBytes ^String payload "UTF-8")))

        digest
        (.digest md)]
    (.encodeToString (Base64/getUrlEncoder) digest)))

(defn get-codec
  "Returns the Parquet compression codec for the given keyword.

   Supported values:
   - :uncompressed - No compression
   - :gzip - GZIP compression (default if unknown)

   Returns CompressionCodecName enum value."
  [compression]
  (case compression
    :uncompressed CompressionCodecName/UNCOMPRESSED
    :gzip CompressionCodecName/GZIP
    CompressionCodecName/UNCOMPRESSED))

(defn write-parquet-bytes
  "Writes rows to Parquet format in memory and returns the byte array.

   Arguments (as a map):
   - :table-name (required) - Name for the table (stored in metadata)
   - :schema (required) - Schema definition map with :description and :columns
   - :rows (required) - Sequence of row maps with string keys matching column names
   - :compression (optional) - Compression codec (:gzip or :uncompressed, default :gzip)
   - :schema-version (optional) - Version string stored in Parquet key/value metadata

   Returns byte[] containing the Parquet file contents."
  [{:keys [table-name schema rows compression schema-version],
    :or {compression :gzip}}]
  (let [parquet-schema
        (build-parquet-schema table-name schema)

        baos
        (ByteArrayOutputStream.)

        output-file
        (create-in-memory-output-file baos)

        conf
        (Configuration.)

        write-support
        (create-write-support parquet-schema (:columns schema))

        filtered-rows
        (mapv #(filter-row-to-schema % schema) rows)

        metadata
        (cond-> (reduce
                 (fn [m [col-name col-type description requirement & opts]]
                   (let [opts-map
                         (apply hash-map opts)

                         enum-values
                         (if (= col-type :boolean)
                           ["TRUE" "FALSE"]
                           (:enum opts-map))]
                     (cond-> m
                       true (assoc (str "column." col-name ".description")
                                   description)
                       true (assoc (str "column." col-name ".requirement")
                                   (name requirement))
                       enum-values (assoc (str "column." col-name ".enum")
                                          (pr-str enum-values)))))
                 {"table.name" table-name,
                  "table.description" (:description schema),
                  "schema.fingerprint" (schema-fingerprint schema)}
                 (:columns schema))
          schema-version (assoc "schema.version" (str schema-version)))

        builder-class
        (proxy [org.apache.parquet.hadoop.ParquetWriter$Builder] [^OutputFile
                                                                  output-file]
          (getWriteSupport [_conf] write-support)
          (self [] this))

        codec
        (get-codec compression)]
    (log/infof "Writing %d rows to in-memory Parquet for table: %s"
               (count filtered-rows)
               table-name)
    (with-open [writer
                (-> builder-class
                    (.withConf conf)
                    (.withCompressionCodec codec)
                    (.withExtraMetaData metadata)
                    (.build))]
      (doseq [row
              filtered-rows]
        (.write writer row)))
    (.toByteArray baos)))

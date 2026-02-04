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

   Memory considerations:
   - Rows are consumed lazily (one at a time), so prefer passing lazy sequences
   - Parquet writer buffers up to one row group (~128MB) internally
   - Output byte[] is fully materialized in memory

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
           [org.apache.parquet.hadoop ParquetFileReader ParquetFileWriter]
           [org.apache.parquet.hadoop.api WriteSupport
            WriteSupport$WriteContext]
           [org.apache.parquet.hadoop.metadata CompressionCodecName]
           [org.apache.parquet.io InputFile OutputFile PositionOutputStream
            SeekableInputStream]
           [org.apache.parquet.io.api Binary]
           [org.apache.parquet.schema LogicalTypeAnnotation MessageType
            PrimitiveType$PrimitiveTypeName Type$Repetition Types]))

(set! *warn-on-reflection* true)

(def ^:private parquet-mime-type "application/vnd.apache.parquet")

(def ^:private binary-true
  "Deferred Binary instance for boolean TRUE (GraalVM compatible)."
  (delay (Binary/fromConstantByteArray (.getBytes "TRUE" "UTF-8"))))

(def ^:private binary-false
  "Deferred Binary instance for boolean FALSE (GraalVM compatible)."
  (delay (Binary/fromConstantByteArray (.getBytes "FALSE" "UTF-8"))))

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
  "Convert YYYY-MM-DD date string to days since Unix epoch (1970-01-01).
   Uses a cache to avoid repeated parsing of the same date strings within a write."
  [^String date-str ^java.util.Map cache]
  (if-let [cached
           (.get cache date-str)]
    (long cached)
    (let [days
          (.toEpochDay (java.time.LocalDate/parse date-str))]
      (.put cache date-str days)
      days)))

(defn- precompute-column-metadata
  "Pre-computes column metadata to avoid repeated parsing in the write loop.
   Returns a vector of tuples for fast indexed access (no keyword lookups):
   [idx name type required? enum-set enum-binaries]

   Using tuples instead of maps eliminates KeywordLookupSite overhead in the
   tight write loop - vector destructuring uses nth which is O(1) array access."
  [column-defs]
  (mapv (fn [idx [col-name col-type _ requirement & opts]]
          (let [opts-map
                (when (seq opts) (apply hash-map opts))

                enum-values
                (:enum opts-map)

                ;; Pre-compute Binary instances for enum values using

                ;; fromConstantByteArray. This avoids UTF-8 encoding

                ;; on every write and makes Binary.copy() free

                enum-binaries
                (when enum-values
                  (into {}
                        (map (fn [^String v] [v
                                              (Binary/fromConstantByteArray
                                               (.getBytes v "UTF-8"))]))
                        enum-values))]
            ;; Return tuple:
            ;; [idx name type required? enum-set enum-binaries]
            [idx col-name col-type (= requirement :required)
             (when enum-values (set enum-values)) enum-binaries]))
        (range)
        column-defs))

(defn- build-column-lookup
  "Builds a HashMap from column-name to metadata tuple for O(1) lookup.
   Used for sparse row iteration optimization."
  ^java.util.HashMap [column-metadata]
  (let [m
        (java.util.HashMap. (count column-metadata))]
    (doseq [[_idx col-name :as tuple]
            column-metadata]
      (.put m col-name tuple))
    m))

(defn- extract-required-columns
  "Extracts required column metadata tuples into a vector for validation."
  [column-metadata]
  (into []
        (filter (fn [[_idx _name _type required?]] required?))
        column-metadata))

(defn- write-column-value
  "Writes a single column value to the record consumer.
   Takes individual column metadata fields instead of a map to avoid keyword lookups."
  [^org.apache.parquet.io.api.RecordConsumer rc idx ^String col-name col-type
   enum-set enum-binaries value ^java.util.Map date-cache]
  (.startField rc col-name (int idx))
  ;; Numeric types handled first - no string conversion needed
  (case col-type
    :double (.addDouble rc (double value))
    :long (.addLong rc (long value))
    ;; Date uses cached parsing
    :date (.addInteger rc
                       (int (date-string->days-since-epoch (str value)
                                                           date-cache)))
    ;; Boolean uses deferred Binary instances for GraalVM compatibility
    :boolean (.addBinary rc (if value @binary-true @binary-false))
    ;; String-based types - convert to string once
    (let [string-value
          (str value)]
      (case col-type
        :string (.addBinary rc (Binary/fromString string-value))
        :enum
        (do
          (when (and enum-set (not (contains? enum-set string-value)))
            (throw
             (ex-info
              (format
               "Invalid enum value '%s' for column '%s' (allowed: %s)"
               string-value
               col-name
               enum-set)
              {:column col-name, :value string-value, :allowed enum-set})))
          (if-let [cached-binary
                   (get enum-binaries string-value)]
            (.addBinary rc cached-binary)
            (.addBinary rc (Binary/fromString string-value))))
        :uuid (.addBinary rc (Binary/fromString string-value))
        ;; Default case for unknown types
        (.addBinary rc (Binary/fromString string-value)))))
  (.endField rc col-name (int idx)))

(defn- write-record-sparse!
  "Writes a single record to the RecordConsumer using sparse iteration.
   Instead of iterating all schema columns (O(schema-size)), iterates only
   the keys present in the record (O(row-keys)).

   For schemas with many optional columns and sparse data, this is significantly
   faster as it avoids checking columns that are nil.

   required-columns: vector of required column metadata tuples
   column-lookup: HashMap from col-name -> metadata tuple
   record: the row data map"
  [^org.apache.parquet.io.api.RecordConsumer rc required-columns
   ^java.util.HashMap column-lookup record ^java.util.Map date-cache
   table-schema]
  (.startMessage rc)
  ;; First, validate and write required columns
  (let [n-required
        (count required-columns)]
    (loop [i
           0]
      (when (< i n-required)
        (let [[idx col-name col-type _required? enum-set enum-binaries]
              (nth required-columns i)

              value
              (get record col-name)]
          (if (some? value)
            (write-column-value rc
                                idx
                                col-name
                                col-type
                                enum-set
                                enum-binaries
                                value
                                date-cache)
            (throw (ex-info
                    (format
                     "Required column '%s' in table-schema '%s' has nil value"
                     col-name
                     table-schema)
                    {:column col-name, :table-schema table-schema}))))
        (recur (unchecked-inc i)))))
  ;; Then iterate record entries for optional columns
  (doseq [[col-name value]
          record]
    (when (some? value)
      (when-let [[idx _name col-type required? enum-set enum-binaries]
                 (.get column-lookup col-name)]
        ;; Skip required columns - already written above
        (when-not required?
          (write-column-value rc
                              idx
                              col-name
                              col-type
                              enum-set
                              enum-binaries
                              value
                              date-cache)))))
  (.endMessage rc))

(defn- write-record!
  "Writes a single record to the RecordConsumer.
   Column metadata is a vector of tuples: [idx name type required? enum-set enum-binaries]
   Using tuple destructuring avoids keyword lookup overhead in the tight loop."
  [^org.apache.parquet.io.api.RecordConsumer rc column-metadata record
   ^java.util.Map date-cache table-schema]
  (.startMessage rc)
  (let [len
        (count column-metadata)]
    (loop [i
           0]
      (when (< i len)
        (let [[idx col-name col-type required? enum-set enum-binaries]
              (nth column-metadata i)

              value
              (get record col-name)]
          (if (some? value)
            (write-column-value rc
                                idx
                                col-name
                                col-type
                                enum-set
                                enum-binaries
                                value
                                date-cache)
            (when required?
              (throw
               (ex-info
                (format
                 "Required column '%s' in table-schema '%s' has nil value"
                 col-name
                 table-schema)
                {:column col-name, :table-schema table-schema})))))
        (recur (unchecked-inc i)))))
  (.endMessage rc))

(defn- create-write-support
  "Creates a WriteSupport instance with pre-computed column metadata.
   Uses an array to store the RecordConsumer for faster access than volatile.
   Includes a date cache for efficient repeated date string parsing.

   Automatically uses sparse iteration when schema has many optional columns,
   which is faster for schemas with >20 columns where most are nil per row."
  [schema column-defs table-schema]
  (let [column-metadata
        (precompute-column-metadata column-defs)

        ;; For sparse optimization

        required-columns
        (extract-required-columns column-metadata)

        column-lookup
        (build-column-lookup column-metadata)

        ;; Use sparse mode when there are many optional columns

        ;; Threshold: more than 20 total columns with some optional

        use-sparse?
        (and (> (count column-metadata) 20)
             (< (count required-columns) (count column-metadata)))

        ;; Cache for date string -> epoch days conversion

        date-cache
        (java.util.HashMap.)

        ^objects consumer-holder
        (object-array 1)]
    (proxy [WriteSupport] []
      (init [_configuration] (WriteSupport$WriteContext. schema {}))
      (prepareForWrite [record-consumer]
        (aset consumer-holder 0 record-consumer))
      (write [record]
        (let [^org.apache.parquet.io.api.RecordConsumer rc
              (aget consumer-holder 0)]
          (if use-sparse?
            (write-record-sparse! rc
                                  required-columns
                                  column-lookup
                                  record
                                  date-cache
                                  table-schema)
            (write-record! rc
                           column-metadata
                           record
                           date-cache
                           table-schema)))))))

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

(defn- create-in-memory-input-file
  "Creates an in-memory InputFile implementation that reads from a byte array.
   Used for merging parquet chunks without writing to disk."
  ^InputFile [^bytes data]
  (reify
    InputFile
    (getLength [_] (long (alength data)))
    (newStream [_]
      (let [pos
            (long-array 1)]
        (proxy [SeekableInputStream] []
          (getPos [] (aget pos 0))
          (seek [^long new-pos] (aset pos 0 new-pos))
          (read
            ([]
             (let [p
                   (aget pos 0)]
               (if (>= p (alength data))
                 -1
                 (let [b
                       (bit-and (int (aget data (int p))) 0xFF)]
                   (aset pos 0 (unchecked-inc p))
                   b))))
            ([^bytes buf off len]
             (let [p
                   (aget pos 0)

                   remaining
                   (- (alength data) p)

                   to-read
                   (min len remaining)]
               (if (<= to-read 0)
                 -1
                 (do
                   (System/arraycopy data (int p) buf (int off) (int to-read))
                   (aset pos 0 (unchecked-add p (long to-read)))
                   to-read)))))
          (readFully
            ([buf-or-bytes]
             (if (instance? java.nio.ByteBuffer buf-or-bytes)
                 ;; ByteBuffer overload: readFully(ByteBuffer buf)
               (let [^java.nio.ByteBuffer buf
                     buf-or-bytes

                     p
                     (aget pos 0)

                     len
                     (.remaining buf)]
                 (.put buf data (int p) len)
                 (aset pos 0 (unchecked-add p (long len))))
                 ;; byte[] overload: readFully(byte[] buf)
               (let [^bytes arr
                     buf-or-bytes

                     p
                     (aget pos 0)

                     len
                     (alength arr)]
                 (System/arraycopy data (int p) arr 0 len)
                 (aset pos 0 (unchecked-add p (long len))))))
            ([^bytes buf start len]
               ;; byte[] overload: readFully(byte[] buf, int start, int
               ;; len)
             (let [p
                   (aget pos 0)]
               (System/arraycopy data (int p) buf (int start) (int len))
               (aset pos 0 (unchecked-add p (long len))))))
          (close []))))))

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

(defn- create-optimized-configuration
  "Creates a Hadoop Configuration optimized for write-only Parquet exports.

   Disables page checksums and column statistics since these files are
   write-once exports that don't use predicate pushdown on read."
  ^Configuration []
  (doto (Configuration.)
    (.setBoolean "parquet.page.write-checksum.enabled" false)
    (.setBoolean "parquet.column.statistics.enabled" false)))

(defn- build-parquet-metadata
  "Builds the key-value metadata map for Parquet files.
   Includes table info, schema fingerprint, column descriptions, and optional version."
  [table-name schema schema-version table-schema]
  (cond-> (reduce (fn [m [col-name col-type description requirement & opts]]
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
    schema-version (assoc "schema.version" (str schema-version))
    table-schema (assoc "table.schema" (str table-schema))))

(defn- low-cardinality-columns
  "Returns set of column names that benefit from dictionary encoding (enum, boolean)."
  [schema]
  (into #{}
        (comp (filter (fn [[_ col-type & _]] (#{:enum :boolean} col-type)))
              (map first))
        (:columns schema)))

(defn get-codec
  "Returns the Parquet compression codec for the given keyword.

   Supported values:
   - :uncompressed - No compression (dev, best compatibility)
   - :snappy - Snappy compression (fast, moderate compression)
   - :zstd - Zstandard compression (good balance of speed and compression)
   - :gzip - GZIP compression (slow, high compression, good compatibility)

   Returns CompressionCodecName enum value."
  [compression]
  (case compression
    :uncompressed CompressionCodecName/UNCOMPRESSED
    :snappy CompressionCodecName/SNAPPY
    :zstd CompressionCodecName/ZSTD
    :gzip CompressionCodecName/GZIP
    CompressionCodecName/UNCOMPRESSED))

(defn write-parquet-bytes-single
  "Writes rows to Parquet format in memory and returns the byte array.

   Arguments (as a map):
   - :table-name (required) - Name for the table (stored in metadata)
   - :schema (required) - Schema definition map with :description and :columns
   - :rows (required) - Sequence of row maps with string keys matching column names.
                        Lazy sequences are preferred for memory efficiency as rows
                        are consumed one at a time without realizing the full collection.
   - :compression (optional) - Compression codec (:gzip or :uncompressed, default :gzip)
   - :schema-version (optional) - Version string stored in Parquet key/value metadata
   - :table-schema (optional) - Database schema name stored in Parquet key/value metadata

   Memory characteristics:
   - Rows are processed incrementally (lazy seqs supported)
   - Parquet writer buffers up to one row group (~128MB) internally
   - Output byte[] is fully materialized in memory

   Returns byte[] containing the Parquet file contents."
  [{:keys [table-name schema rows compression schema-version table-schema],
    :or {compression :gzip}}]
  (let [parquet-schema
        (build-parquet-schema table-name schema)

        ;; Start with 64KB initial capacity to reduce array copies during

        ;; growth. Parquet files are typically KB-MB in size, so this

        ;; avoids many resizes

        baos
        (ByteArrayOutputStream. 65536)

        output-file
        (create-in-memory-output-file baos)

        conf
        (create-optimized-configuration)

        write-support
        (create-write-support parquet-schema (:columns schema) table-schema)

        metadata
        (build-parquet-metadata table-name schema schema-version table-schema)

        low-card-cols
        (low-cardinality-columns schema)

        builder-class
        (proxy [org.apache.parquet.hadoop.ParquetWriter$Builder] [^OutputFile
                                                                  output-file]
          (getWriteSupport [_conf] write-support)
          (self [] this))

        codec
        (get-codec compression)

        ;; Build writer with selective dictionary encoding

        ^org.apache.parquet.hadoop.ParquetWriter$Builder base-builder
        (-> builder-class
            (.withConf conf)
            (.withCompressionCodec codec)
            ;; Disable dictionary encoding globally - most columns are
            ;; high-cardinality
            ;; (UUIDs, unique strings). Dictionary wastes CPU before
            ;; falling back.
            (.withDictionaryEncoding false)
            ;; Disable validation for write-only exports
            (.withValidation false)
            (.withExtraMetaData metadata))

        ;; Re-enable dictionary encoding for low-cardinality columns

        ;; (enum, boolean)

        ^org.apache.parquet.hadoop.ParquetWriter$Builder configured-builder
        (reduce (fn [^org.apache.parquet.hadoop.ParquetWriter$Builder builder
                     col-name]
                  (.withDictionaryEncoding builder ^String col-name true))
                base-builder
                low-card-cols)]
    (log/debugf "Writing rows to in-memory Parquet for table: %s" table-name)
    (with-open [^org.apache.parquet.hadoop.ParquetWriter writer
                (.build configured-builder)]
      (doseq [row
              rows]
        (.write writer row)))
    (.toByteArray baos)))

(defn partition-rows
  "Partitions a sequence of rows into N roughly equal chunks for parallel processing.
    Returns a vector of vectors with balanced distribution (max-min size difference ≤ 1)."
  [rows num-partitions]
  (let [rows-vec
        (vec rows)

        n
        (count rows-vec)]
    (if (zero? n)
      []
      (let [num-parts
            (min num-partitions n)

            base-size
            (quot n num-parts)

            remainder
            (rem n num-parts)

            partition-sizes
            (vec (concat (repeat remainder (inc base-size))
                         (repeat (- num-parts remainder) base-size)))]
        (loop [row-idx
               0

               part-idx
               0

               result
               []]
          (if (>= part-idx num-parts)
            result
            (let [part-size
                  (long (partition-sizes part-idx))

                  part
                  (vec (subvec rows-vec row-idx (+ row-idx part-size)))]
              (recur (+ row-idx part-size)
                     (inc part-idx)
                     (conj result part)))))))))

(defn- write-chunk-to-bytes
  "Writes a chunk of rows to Parquet format and returns the byte array.
   Used for in-memory parallel processing."
  ^bytes [{:keys [rows schema table-name compression schema-version]}]
  (write-parquet-bytes-single {:table-name table-name,
                               :schema schema,
                               :rows rows,
                               :compression compression,
                               :schema-version schema-version}))

(defn- merge-parquet-bytes-in-memory
  "Merges multiple Parquet byte arrays into a single output byte array.
   Uses ParquetFileWriter for efficient merging without re-serialization.

   Arguments (as a map):
   - :chunks (required) - Collection of byte arrays containing Parquet data
   - :schema (required) - Schema definition map
   - :table-name (required) - Name for the table (in metadata)
   - :schema-version (optional) - Version string stored in Parquet key/value metadata
   - :table-schema (optional) - Database schema name stored in Parquet key/value metadata

   Returns byte[] containing the merged Parquet file."
  ^bytes [{:keys [chunks schema table-name schema-version table-schema]}]
  (when (empty? chunks)
    (throw (IllegalArgumentException. "At least one chunk required for merge")))
  (let [parquet-schema
        (build-parquet-schema table-name schema)

        baos
        (ByteArrayOutputStream. 65536)

        output-file
        (create-in-memory-output-file baos)

        metadata
        (build-parquet-metadata table-name schema schema-version table-schema)

        writer
        (ParquetFileWriter.
         ^OutputFile output-file
         parquet-schema
         org.apache.parquet.hadoop.ParquetFileWriter$Mode/OVERWRITE
         (long (* 128 1024 1024))
         (int 0))]
    (.start writer)
    (doseq [^bytes chunk
            chunks]
      (with-open [reader
                  (ParquetFileReader/open (create-in-memory-input-file chunk))]
        (.appendTo reader writer)))
    (.end writer (java.util.HashMap. ^java.util.Map metadata))
    (.toByteArray baos)))

(defn- write-chunks-parallel-mem
  "Common logic for parallel chunk writing in memory. Returns vector of byte arrays."
  [{:keys [rows-vec schema table-name compression schema-version threads]}]
  (let [partitions
        (partition-rows rows-vec threads)]
    (doall (pmap (fn [row-chunk]
                   (write-chunk-to-bytes {:rows row-chunk,
                                          :schema schema,
                                          :table-name table-name,
                                          :compression compression,
                                          :schema-version schema-version}))
                 partitions))))

(def ^:private parallel-row-threshold
  "Minimum row count for parallel processing to be beneficial.
   Below this threshold, the overhead of partitioning, temp files,
   and merging exceeds the parallelization benefit."
  10000)

(defn write-parquet-bytes
  "Writes rows to Parquet format using parallel multi-threaded processing entirely in memory.
   Partitions are processed as byte arrays in memory and merged in memory.

   For row counts below 10,000 or when receiving a lazy seq, automatically falls back to single-threaded
   write-parquet-bytes-single since parallel overhead exceeds benefit or we don't want to fully realize the data.

   Arguments (as a map):
   - :table-name (required) - Name for the table
   - :schema (required) - Schema definition map
   - :rows (required) - Sequence of row maps
   - :compression (optional) - Compression codec (default :gzip)
   - :schema-version (optional) - Version string for metadata
   - :table-schema (optional) - Database schema name stored in Parquet key/value metadata
   - :threads (optional) - Number of parallel threads (default: available processors)

   Returns byte[] containing the Parquet file contents."
  [{:keys [table-name schema rows compression schema-version table-schema
           threads],
    :or {compression :gzip,
         threads (.. Runtime getRuntime availableProcessors)}}]
  (let [is-lazy?
        ((complement counted?) rows)]
    (if (or is-lazy? (> parallel-row-threshold (count rows)))
      ;; Below threshold or lazy seq passed - going single-threaded
      (write-parquet-bytes-single {:table-name table-name,
                                   :schema schema,
                                   :rows rows,
                                   :compression compression,
                                   :schema-version schema-version,
                                   :table-schema table-schema})
      ;; Above threshold - parallel in-memory processing
      (let [chunks
            (write-chunks-parallel-mem {:rows-vec rows,
                                        :schema schema,
                                        :table-name table-name,
                                        :compression compression,
                                        :schema-version schema-version,
                                        :threads threads})]
        (merge-parquet-bytes-in-memory {:chunks chunks,
                                        :schema schema,
                                        :table-name table-name,
                                        :schema-version schema-version,
                                        :table-schema table-schema})))))

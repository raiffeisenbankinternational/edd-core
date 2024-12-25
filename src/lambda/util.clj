(ns lambda.util
  (:refer-clojure :exclude [update-keys])
  (:require [clojure.java.io :as io]
            [clojure.set :as clojure-set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [java-http-clj.core :as http]
            [jsonista.core :as json]
            [lambda.aes :as aes]
            [lambda.gzip :as gzip]
            [lambda.request :as request])
  (:import (clojure.lang Keyword)
           (com.fasterxml.jackson.core JsonGenerator)
           (com.fasterxml.jackson.databind ObjectMapper)
           (com.fasterxml.jackson.databind.module SimpleModule)
           (java.io File BufferedReader InputStream)
           (java.net URLEncoder)
           (java.nio.charset Charset)
           (java.nio.charset StandardCharsets)
           (java.time OffsetDateTime)
           (java.time.format DateTimeFormatter)
           (java.util UUID Date Base64)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (jsonista.jackson FunctionalUUIDKeySerializer)
           (mikera.vectorz AVector)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def offset-date-time-format "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

(defn decode-json-special
  [^String v]
  (case (first v)
    \: (if (str/starts-with? v "::")
         (subs v 1)
         (keyword (subs v 1)))
    \# (if (str/starts-with? v "##")
         (subs v 1)
         (UUID/fromString (subs v 1)))
    v))

(def edd-core-module
  (doto (SimpleModule. "EddCore")
    (.addKeySerializer
     UUID
     (FunctionalUUIDKeySerializer. (partial str "#")))))

(def ^ObjectMapper json-mapper
  (json/object-mapper
   {:modules [edd-core-module]
    :decode-key-fn
    (fn [v]
      (case (first v)
        \# (if (str/starts-with? v "##")
             (subs v 1)
             (UUID/fromString (subs v 1)))
        (keyword v)))
    :decode-fn
    (fn [v]
      (condp instance? v
        String (decode-json-special v)
        v))
    :encoders      {String         (fn [^String v ^JsonGenerator jg]
                                     (cond
                                       (str/starts-with? v ":")
                                       (.writeString jg (str ":" v))
                                       (str/starts-with? v "#")
                                       (.writeString jg (str "#" v))
                                       :else (.writeString jg v)))
                    BufferedReader (fn [^BufferedReader v ^JsonGenerator jg]
                                     (.writeString jg "BufferedReader"))
                    UUID           (fn [^UUID v ^JsonGenerator jg]
                                     (.writeString jg (str "#" v)))
                    Keyword        (fn [^Keyword v ^JsonGenerator jg]
                                     (.writeString jg (str ":" (name v))))
                    AVector        (fn [^AVector v ^JsonGenerator jg]
                                     (.writeStartArray jg)
                                     (doseq [n v]
                                       (.writeNumber jg (str n)))
                                     (.writeEndArray jg))}}))

(defn date-time
  (^OffsetDateTime [] (OffsetDateTime/now))
  (^OffsetDateTime [^String value] (OffsetDateTime/parse value)))

(defn date->string
  ([] (.format (date-time) (DateTimeFormatter/ofPattern offset-date-time-format)))
  ([^OffsetDateTime date] (.format date (DateTimeFormatter/ofPattern offset-date-time-format))))

(defn get-current-time-ms
  []
  (System/currentTimeMillis))

(defn is-in-past
  [^Date date]
  (.before date (new Date)))

(defn to-edn
  [json]
  (json/read-value json json-mapper))

(defn to-json
  ^String [edn]
  (json/write-value-as-string edn json-mapper))

(defn to-json-bytes
  ^bytes [edn]
  (json/write-value-as-bytes edn json-mapper))

(defn to-json-out
  "
  Write a value into a destination that can be
  a file, an output stream, a writer, etc.
  "
  [dest edn]
  (json/write-value dest edn json-mapper))

(defn wrap-body [request]
  (cond
    (:body request) request
    (:form-params request) request
    :else {:body (to-json request)}))

(defn url-encode
  [^String message]
  (let [^Charset charset (Charset/forName "UTF-8")]
    (URLEncoder/encode message charset)))

(defn nested-param
  "Source: https://github.com/http-kit/http-kit"
  [params]
  (walk/prewalk (fn [d]
                  (if (and (vector? d) (map? (second d)))
                    (let [[fk m] d]
                      (reduce (fn [m [sk v]]
                                (assoc m (str (name fk) \[ (name sk) \]) v))
                              {} m))
                    d))
                params))

(defn query-string
  "Returns URL-encoded query string for given params map.
   Source: https://github.com/http-kit/http-kit"
  [m]
  (let [m (nested-param m)
        param (fn [k v]  (str (url-encode (name k)) "=" (url-encode v)))
        join  (fn [strs] (str/join "&" strs))]
    (join (for [[k v] m] (if (sequential? v)
                           (join (map (partial param k) (or (seq v) [""])))
                           (param k v))))))

(defn update-keys
  [m f]
  (let [ret (persistent!
             (reduce-kv (fn [acc k v] (assoc! acc (f k) v))
                        (transient {})
                        m))]
    (with-meta ret (meta m))))

(defn header->kw [^String header]
  (-> header str/trim str/lower-case keyword))

(defn maybe-ungzip-res [res]
  (if (gzip/response-gzipped? res)
    (gzip/ungzip-response res)
    res))

(defn stream->string ^String [^InputStream stream]
  (new String
       (.readAllBytes stream)
       StandardCharsets/UTF_8))

(defn stream->bytes ^bytes [^InputStream stream]
  (.readAllBytes stream))

(defn request
  [{:as req :keys [as]} & _rest]
  ;; Preserve the origin `:as` argument.
  ;; When emitting a request, specify `:input-stream`.
  (let [opt {:as :input-stream}
        req (clojure-set/rename-keys req {:url :uri})
        trace-headers (get @request/*request* :trace-headers)
        req (cond-> req
              (:headers req) (update-in [:headers]
                                        #(-> %
                                             (dissoc "Host")
                                             (merge trace-headers)))
              (:query-params req) (update-in [:uri]
                                             #(str %
                                                   "?"
                                                   (query-string (:query-params req))))
              (:form-params req) (assoc-in [:headers "Content-Type"] "application/x-www-form-urlencoded")
              (:form-params req) (assoc-in [:body] (query-string (:form-params req)))

              :finally (update :headers assoc "Accept-Encoding" "gzip"))
        res (-> req
                (http/send opt)
                (update :headers update-keys header->kw)
                (maybe-ungzip-res))]

    ;; Now when the body has been ungzipped, apply the origin
    ;; `:as` coercion. Without gzip being applied first, the
    ;; coercion will be broken.
    (case as

      (nil :string)
      (update res :body stream->string)

      (:stream :input-stream)
      res

      (:byte-array)
      (update res :body stream->bytes))))

(defn http-get
  [url req & {:keys [raw]}]
  (let [resp (request
              (assoc req
                     :method :get
                     :url url))]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-delete
  [url req & {:keys [raw]}]
  (let [resp (request
              (assoc req
                     :method :delete
                     :url url))]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-put
  [url req & {:keys [raw]}]
  (log/debug url req)
  (let [req (wrap-body req)
        resp (request
              (assoc req
                     :method :put
                     :url url))]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-post
  [url req & {:keys [raw]}]
  (log/debug url req)
  (let [req (wrap-body req)
        resp (request
              (assoc req
                     :method :post
                     :url url))]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn get-env
  [name & [default]]
  (get (System/getenv) name default))

(defn get-property
  [name & [default]]
  (get (System/getProperties) name default))

(defn escape
  [value]
  (str/replace value "\"" "\\\""))

(defn decrypt
  [body ^String name]
  (log/debug "Decrypting")
  (let [context (get-env "ConfigurationContext")]
    (if (and context
             (.contains name "secret"))
      (let [context (str/split context #":")
            iv (first context)
            key (second context)]
        (aes/decrypt (str/replace body #"\n" "")
                     key
                     iv))
      body)))

(defn load-config
  [name]
  (log/debug "Loading config name:" name)
  (let [file (io/as-file name)
        temp-file (io/as-file (str "/tmp/" name))
        classpath (io/as-file
                   (io/resource
                    name))]
    (to-edn
     (cond
       (.exists ^File file) (do
                              (log/debug "Loading from file config:" name)
                              (-> file
                                  (slurp)
                                  (decrypt name)))
       (.exists ^File temp-file) (do
                                   (log/info "Loading from /tmp/" name)
                                   (-> temp-file
                                       (slurp)))
       :else (do
               (log/debug "Loading config from classpath:" name)
               (-> classpath
                   (slurp)
                   (decrypt name)))))))

(defn base64encode
  [^String to-encode]
  (.encodeToString (Base64/getEncoder)
                   (.getBytes to-encode "UTF-8")))

(defn base64decode
  [^String to-decode]
  (String. (.decode
            (Base64/getDecoder)
            to-decode) "UTF-8"))

(defn base64URLdecode
  [^String to-decode]
  (String. (.decode
            (Base64/getUrlDecoder)
            to-decode) "UTF-8"))

(def ^:dynamic *cache*)

(defn hmac-sha256
  [^String secret ^String message]
  (let [mac (Mac/getInstance "HmacSHA256")
        secret-key-spec (new SecretKeySpec
                             (.getBytes secret "UTF-8")
                             "HmacSHA256")
        message-bytes (.getBytes message "UTF-8")]
    (.init mac secret-key-spec)
    (->> message-bytes
         (.doFinal mac)
         (.encodeToString (Base64/getEncoder)))))

(defmacro d-time
  "Evaluates expr and logs time it took.  Returns the value of
 expr."
  {:added "1.0"}
  [message & expr]
  `(do
     (let [start# (. System (nanoTime))
           mem# (long
                 (/ (long (- (.totalMemory (Runtime/getRuntime))
                             (.freeMemory (Runtime/getRuntime))))
                    1048576.0))

           ignore# (log/info (str "START " ~message "; memory(mb): " mem#))
           ret# (do
                  ~@expr)]
       (log/info (str
                  "END " ~message "; "
                  "elapsed(msec): " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) "; "
                  "memory(mb): " (str mem#
                                      " -> "
                                      (long
                                       (/ (long (- (.totalMemory (Runtime/getRuntime))
                                                   (.freeMemory (Runtime/getRuntime))))
                                          1048576.0)))))
       ret#)))

(defn fix-keys
  "This is used to represent as close as possible when we store
  to external storage as JSON. Because we are using :keywordize keys
  for convenience. Problem is when map keys are in aggregate stored as strings.
  Then when they are being read from storage they are being keywordized.
  This is affecting when we are caching aggregate between calls because in
  this case cached aggregate would not represent real aggregate without cache.
  Other scenario is for tests because in tests we would get aggregate with string
  keys while in real scenario we would have keys as keywords."
  [val]
  (-> val
      (to-json)
      (to-edn)))

(defn hex-str-to-bit-str
  [hex]
  (case hex
    "0" "0000"
    "1" "0001"
    "2" "0010"
    "3" "0011"
    "4" "0100"
    "5" "0101"
    "6" "0110"
    "7" "0111"
    "8" "1000"
    "9" "1001"
    "a" "1010"
    "b" "1011"
    "c" "1100"
    "d" "1101"
    "e" "1110"
    "f" "1111"))

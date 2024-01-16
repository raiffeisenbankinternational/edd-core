(ns lambda.codec
  (:import
   java.io.ByteArrayOutputStream
   java.io.ByteArrayInputStream
   java.nio.charset.StandardCharsets
   java.util.Base64
   java.util.zip.GZIPOutputStream
   java.util.zip.GZIPInputStream))

(defn string->bytes ^bytes [^String string]
  (.getBytes string StandardCharsets/UTF_8))

(defn bytes->string ^String [^bytes bytea]
  (new String bytea StandardCharsets/UTF_8))

(defn bytes->gzip ^bytes [^bytes bytea]
  (let [out (new ByteArrayOutputStream)
        gzip (new GZIPOutputStream out)]
    (.write gzip bytea)
    (.finish gzip)
    (.toByteArray out)))

(defn gzip->bytes ^bytes [^bytes bytea]
  (let [in (new ByteArrayInputStream bytea)
        gzip (new GZIPInputStream in)]
    (.readAllBytes gzip)))

(defn bytes->base64 ^bytes [^bytes bytea]
  (let [encoder (Base64/getEncoder)]
    (.encode encoder bytea)))

(defn bytes->base64-string ^String [^bytes bytea]
  (let [encoder (Base64/getEncoder)]
    (.encodeToString encoder bytea)))

(defn base64->bytes ^bytes [^bytes bytea]
  (let [decoder (Base64/getDecoder)]
    (.decode decoder bytea)))

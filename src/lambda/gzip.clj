(ns lambda.gzip
  "
  Gzip-related functions and predicates.
  "
  (:require
   [lambda.codec :as codec]
   [clojure.string :as str])
  (:import
   java.util.zip.GZIPInputStream))

(defn response-gzipped? [response]
  (some-> response
          :headers
          :content-encoding
          str/trim
          str/lower-case
          (= "gzip")))

(defn ungzip-response [response]
  (update response
          :body
          (fn [body]
            (new GZIPInputStream body))))

(defn accepts-gzip? [request]
  (when-let [accept-encoding
             (or (get-in request [:headers :Accept-Encoding])
                 (get-in request [:headers :accept-encoding]))]
    (-> accept-encoding
        str/lower-case
        (str/includes? "gzip"))))

(defn sub-response-gzip [^String content]
  (let [encoded (-> content
                    codec/string->bytes
                    codec/bytes->gzip
                    codec/bytes->base64-string)]
    {:isBase64Encoded true
     :body encoded
     :headers {"Content-Encoding" "gzip"}}))

(defn sub-response [^String content]
  {:isBase64Encoded false
   :body content})

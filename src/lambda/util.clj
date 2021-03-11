(ns lambda.util
  (:require [jsonista.core :as json]
            [clojure.tools.logging :as log]
            [org.httpkit.client :as http]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [lambda.aes :as aes])
  (:import (java.time OffsetDateTime)
           (java.time.format DateTimeFormatter)
           (java.io File BufferedReader)
           (java.util UUID Date Base64)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (com.fasterxml.jackson.core JsonGenerator)
           (clojure.lang Keyword)
           (com.fasterxml.jackson.databind ObjectMapper)
           (java.nio.charset Charset)
           (java.net URLEncoder)))

(def offset-date-time-format "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

(def ^ObjectMapper json-mapper
  (json/object-mapper
   {:decode-key-fn true
    :decode-fn
    (fn [v]
      (condp instance? v
        String (case (first v)
                 \: (if (str/starts-with? v "::")
                      (subs v 1)
                      (keyword (subs v 1)))
                 \# (if (str/starts-with? v "##")
                      (subs v 1)
                      (UUID/fromString (subs v 1)))
                 v)
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
                                     (.writeString jg (str ":" (name v))))}}))

(defn date-time
  ([] (OffsetDateTime/now))
  ([^String value] (OffsetDateTime/parse value)))

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
  [edn]
  (json/write-value-as-string edn json-mapper))

(defn wrap-body [request]
  (cond
    (:body request) request
    (:form-params request) request
    :else {:body (to-json request)}))

(defn http-get
  [url request & {:keys [raw]}]
  (let [resp @(http/get url request)]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-delete
  [url request & {:keys [raw]}]
  (let [resp @(http/delete url request)]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-put
  [url request & {:keys [raw]}]
  (log/debug url request)
  (let [req (wrap-body request)
        resp @(http/put url req)]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn http-post
  [url request & {:keys [raw]}]
  (log/debug url request)
  (let [req (wrap-body request)
        resp @(http/post url req)]
    (log/debug "Response" resp)
    (if raw
      resp
      (assoc resp
             :body (to-edn (:body resp))))))

(defn get-env
  [name & [default]]
  (get (System/getenv) name default))

(defn escape
  [value]
  (str/replace value "\"" "\\\""))

(defn decrypt
  [body name]
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
        classpath (io/as-file
                   (io/resource
                    name))]
    (to-edn
     (if (.exists ^File file)
       (do
         (log/debug "Loading from file config:" name)
         (-> file
             (slurp)
             (decrypt name)))
       (do
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

(defn url-encode
  [^String message]
  (let [^Charset charset (Charset/forName "UTF-8")]
    (URLEncoder/encode message charset)))
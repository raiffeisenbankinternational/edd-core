(ns edd.json.core
  "
  JSON-related utilites transferred from the
  lambda.util namespace into a separate module.

  The idea behind this is to not depend on the
  whole edd-core library when only JSON-related
  capabilities are required.
  "
  (:require
   [clojure.string :as str]
   [jsonista.core :as json])
  (:import
   (clojure.lang Keyword)
   (com.fasterxml.jackson.core JsonGenerator)
   (com.fasterxml.jackson.databind ObjectMapper)
   (com.fasterxml.jackson.databind.module SimpleModule)
   (java.io BufferedReader)
   (java.util UUID)
   (jsonista.jackson FunctionalUUIDKeySerializer)
   (mikera.vectorz AVector)))

(set! *warn-on-reflection* true)

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

    :encoders
    {String
     (fn [^String v ^JsonGenerator jg]
       (cond
         (str/starts-with? v ":")
         (.writeString jg (str ":" v))
         (str/starts-with? v "#")
         (.writeString jg (str "#" v))
         :else (.writeString jg v)))

     BufferedReader
     (fn [^BufferedReader _v ^JsonGenerator jg]
       (.writeString jg "BufferedReader"))

     UUID
     (fn [^UUID v ^JsonGenerator jg]
       (.writeString jg (str "#" v)))

     Keyword
     (fn [^Keyword v ^JsonGenerator jg]
       (.writeString jg (str ":" (name v))))

     AVector
     (fn [^AVector v ^JsonGenerator jg]
       (.writeStartArray jg)
       (doseq [n v]
         (.writeNumber jg (str n)))
       (.writeEndArray jg))}}))

(defn to-edn
  "
  Read a source into a Clojure data structure.
  A source can be a string, a byte array, an
  input stream, a file and so on.
  "
  [src]
  (json/read-value src json-mapper))

(defn to-json
  "
  Turn an arbitrary Clojure data structure into
  a JSON string.
  "
  ^String [data]
  (json/write-value-as-string data json-mapper))

(defn to-json-bytes
  "
  Turn an arbitrary Clojure data structure into
  a byte array with a JSON payload (in UTF-8).
  "
  ^bytes [data]
  (json/write-value-as-bytes data json-mapper))

(defn to-json-out
  "
  Write a value into an ouput that can be a file,
  an output stream, a writer, etc.
  "
  [out data]
  (json/write-value out data json-mapper))

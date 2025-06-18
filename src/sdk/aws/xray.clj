(ns sdk.aws.xray
  (:require [clojure.string :as string]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn generate-xray-segment-id
  []
  (let [hex-chars "0123456789abcdef"
        segment-id (string/join ""
                                (repeatedly 16
                                            #(str
                                              (nth hex-chars
                                                   (rand-int 16)))))]
    (str "1-" segment-id "-" (str (System/currentTimeMillis) "0000"))))

(defn parse-xray-token
  [token]
  (let [token (string/trim token)
        parts (string/split token #";")]
    (reduce
     (fn [p v]
       (let [[keyname value] (string/split v #"=")]
         (assoc p
                (keyword keyname)
                value)))
     {}
     parts)))

(defn render-xray-token
  [token]
  (let [parts (map
               (fn [[keyname value]]
                 (str (name keyname) "=" value))
               token)]
    (string/join ";" parts)))


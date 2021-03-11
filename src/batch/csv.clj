(ns batch.csv
  (:require [clojure.data.csv :as csv]
            [clojure.string :as str]))

(defn remove-m
  [column]
  (apply str (filter (fn [c] (not= c \return)) column)))

(defn sanitize
  [column]
  (-> column
      (str/lower-case)
      (str/trim)
      (str/replace "_" "-")
      (str/replace " " "-")
      (str/replace "\r" "")
      ; Strange regex to remove invisible characters
      (str/replace #"[^\x00-\x7F]" "")
      (remove-m)
      (str/trim-newline)))

(defn convert-column-names
  [column keep-original]
  (keyword (if keep-original
             (str/replace column #"[^\x00-\x7F]" "")
             (sanitize column))))

(defn csv-data->maps
  [csv-data keep-original]
  (map zipmap
       (->> (first csv-data)                                ;; First row is the header
            (map
             #(convert-column-names % keep-original))      ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

(defn parse-csv
  [stream & [sep keep-original]]
  (csv-data->maps
   (csv/read-csv stream
                 :separator (if sep sep \;))
   (if keep-original
     keep-original
     false)))

(ns edd.modules.import.util
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))


(defn to-csv
  [data]
  (io/reader
    (char-array
      (str
        (str/join "|" (map
                        #(name %)
                        (keys (first data))))
        "\n"
        (str/join
          "\n"
          (map
            #(str/join "|" (vals %))
            data))))))

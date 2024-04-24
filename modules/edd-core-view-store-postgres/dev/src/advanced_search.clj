(ns advanced-search
  "
  A set of tools to parse and analyze advanced search
  entries collected from CloudWatch logs.
  "
  (:import
   java.util.zip.GZIPInputStream)
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.parser :as parser]))

(defn parse-row [[service query]]
  [(read-string service)
   (-> query
       (str/replace #"\\\"" "\"")
       read-string)])

(defn load-advanced-search []
  (with-open [reader
              (-> "cloudwatch/dev19-advanced-search.csv.gz"
                  io/resource
                  io/input-stream
                  GZIPInputStream.
                  io/reader)]
    (->> reader
         (csv/read-csv)
         (rest)
         (map parse-row)
         (vec))))

(defn find-by-service [service]
  (->> (load-advanced-search)
       (filter (fn [[_service]]
                 (= _service service)))
       (map second)
       (set)))

(defn find-advanced-sort []
  (->> (load-advanced-search)
       (filter (fn [[service query]]
                 (:sort query)))
       (map (fn [[service query]]
              [service (:sort query)]))
       (set)))

(defn find-advanced-search []
  (->> (load-advanced-search)
       (keep (fn [[service query]]
               (:search query)))
       (set)))

(defn find-wildcards
  [& [{:keys [service]}]]

  (cond->> (load-advanced-search)

    :always
    (filter (fn [[service query]]
              (-> query str (str/includes? "wildcard"))))

    service
    (filter (fn [[_service _]]
              (= _service service)))

    :finally
    (set)))

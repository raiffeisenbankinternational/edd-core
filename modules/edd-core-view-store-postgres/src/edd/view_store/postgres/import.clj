(ns edd.view-store.postgres.import
  "
  Import data from Open Search to Postgres.
  Use it in Jupyter notebook.
  "
  (:import
   clojure.lang.Keyword
   java.io.File
   java.time.OffsetTime
   java.util.Map
   java.util.UUID)
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [edd.io.core :as edd.io]
   [edd.postgres.pool :as pool :refer [*DB*]]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.common :refer [error!]]
   [lambda.elastic :as el]
   [lambda.util :as util]))

(set! *warn-on-reflection* true)

(defmacro log
  "
  A simple logging macro for Jupyter notebook. The standard
  `clojure.tools.logging` namespace makes no output in Jupyter.
  "
  ([message]
   `(println (str (.withNano (OffsetTime/now) 0))
             (ns-name *ns*)
             ~message))
  ([template & args]
   `(log (format ~template ~@args))))

(defn ->csv-rows [aggregates]
  (map (juxt :id util/to-json) aggregates))

(defn ->snake-case [x]
  (-> x name (str/replace #"-" "_")))

(defn make-index [realm service-name]
  (keyword (format "%s_%s"
                   (->snake-case realm)
                   (->snake-case service-name))))

(defn os-init-scroll
  "
  Initiate the scrolling API. Return a pair of
  [scroll-id, aggregates]. Accept the chunk size
  and the lifetime of this scroll.
  "
  ([ctx index]
   (os-init-scroll ctx index 1000 "30m"))

  ([ctx index size lifetime]

   (log "OS init scroll, size: %s, lifetime: %s"
        size lifetime)

   (let [{:keys [aws
                 elastic-search]}
         ctx

         path
         (format "/%s/_search" (name index))

         query
         {"size" (str size)
          "scroll" lifetime}

         params
         {:method         "GET"
          :path           path
          :query          query
          :elastic-search elastic-search
          :aws            aws}

         {error :error
          hits :hits
          scroll-id :_scroll_id}
         (el/query params)

         _
         (when error
           (error! "Open Search error: %s, path: %s, query: %s"
                   error path query))

         {:keys [hits]}
         hits

         aggregates
         (mapv :_source hits)]

     (log "OS init scroll: got %s aggregates, scroll id: %s"
          (count aggregates) scroll-id)

     [scroll-id aggregates])))

(defn os-call-scroll
  "
  Having a scroll ID, continually call this function
  to get another chunk of documents. Returns a pair of
  [next-scroll-id, aggregates]
  "
  [ctx scroll-id lifetime]

  (let [{:keys [aws
                elastic-search]}
        ctx

        path
        "/_search/scroll"

        data
        {"scroll_id" scroll-id
         "scroll" lifetime}

        params
        {:method         "GET"
         :path           path
         :body           (util/to-json data)
         :elastic-search elastic-search
         :aws            aws}

        {error :error
         hits :hits
         scroll-id-next :_scroll_id}
        (el/query params)

        _
        (when error
          (error! "Open Search error: %s, path: %s"
                  error path))

        {:keys [hits]}
        hits

        aggregates
        (mapv :_source hits)]

    (log "OS scroll call: got %s aggregates, next scroll id: %s"
         (count aggregates) scroll-id-next)

    [scroll-id-next aggregates]))

(defn os-dump-to-csv
  "
  Having a realm, service name, and other parameters,
  dump aggregates from OpenSearch into a gzipped CSV file.
  The file has two columns: an ID, and the JSON payload.
  There is no a header row in the file.

  Retrun a map with the following keys:

  - :file an instance of java.io.File where the data is stored;
  - :total an number of rows stored in the file.
  "
  [ctx realm service size lifetime]

  (let [file
        (edd.io/get-temp-file "tmp" ".csv.gz")]

    (with-open [out (-> file
                        (edd.io/gzip-output-stream)
                        (io/writer))]
      (let [index
            (make-index realm service)

            [scroll-id aggregates]
            (os-init-scroll ctx index size lifetime)

            total
            (count aggregates)

            _
            (csv/write-csv out (->csv-rows aggregates))

            total
            (loop [total total
                   scroll-id scroll-id]
              (let [[scroll-id-next aggregates]
                    (os-call-scroll ctx scroll-id lifetime)]
                (if (seq aggregates)
                  (do
                    (csv/write-csv out (->csv-rows aggregates))
                    (recur (+ total (count aggregates)) scroll-id-next))
                  total)))]

        {:file file
         :total total}))))

(defn os-import
  "
  Import the OS target index into the PG database.
  - realm is a keyword describing the realm, e.g. :test
  - service-name is a kebab-case keyword, e.g. :glms-facility-svc
  - size is an integer, the chunk size when dumping OpenSearch
    into a CSV file;
  - lifetime is a string like '10m' for OS scroll.

  1) Dump OpenSearch into a gzipped CSV file;
  2) COPY IN the file into a temp table, then UPSERT into the
     primary table handling ID conflicts;
  3) Delete the file.
  "
  [ctx realm service size lifetime]

  (log "Start OS import, realm: %s, service: %s, size: %s, lifetime: %s"
       realm service size lifetime)

  (log "Connection pool has been started")

  (let [{:keys [^File file total]}
        (os-dump-to-csv ctx realm service size lifetime)]

    (log "OpenSearch was dumped into a file, rows: %s, path: %s"
         total, file)

    (pool/with-pool [ctx]
      (with-open [in (edd.io/gzip-input-stream file)]
        (api/copy-in-csv *DB* realm service in)))

    (log "Rows imported: %s" total)

    (.delete file)))

(ns edd.view-store.postgres.fixtures
  (:import
   java.util.zip.GZIPInputStream)
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [edd.db :as db]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.postgres.pool :as pool :refer [*DB*]]
   [lambda.util :as util]
   [next.jdbc :as jdbc]))

(def DOCKER?
  (System/getenv "CHANGE_ID"))

(def USER "postgres")

(def PASS "no-secret")

(def HOST
  (if DOCKER? "postgres" "127.0.0.1"))

(def PORT 5432)

(def pool-opt
  {:dbtype "postgres"
   :dbname USER
   :port PORT
   :host HOST
   :username USER
   :password PASS})

(def TABLES-TO-DROP
  [c/SVC_TEST c/SVC_DIMENSION])

(defn fix-truncate-db [t]
  (doseq [table TABLES-TO-DROP]
    (honey/execute *DB* {:truncate [(api/->table c/TEST_REALM table)]}))
  (t))

(defn fix-with-db [t]
  (with-open [pool (pool/create-pool pool-opt)]
    (binding [*DB* pool]
      (t))))

(defn fix-with-db-init [t]
  (with-redefs [db/init (constantly pool-opt)]
    (t)))

(defn import-local-file [^String local-file]
  (let [reader
        (-> local-file
            (io/file)
            (or (throw (new Exception (format "file %s not found" local-file))))
            (io/input-stream)
            (GZIPInputStream.)
            (io/reader))]
    (with-open [in reader]
      (let [entities
            (->> in
                 (csv/read-csv)
                 (rest)
                 (map first)
                 (map util/to-edn))]
        (api/copy-in *DB*
                     c/TEST_REALM
                     c/SVC_TEST
                     entities)))))

(defn fix-import-entity
  "
  Returns a fixture that imports a gzipped CSV file
  from the test/resources directory.
  "
  [entity-type]
  (let [path
        (format "test/resources/entities/test_%s.csv.gz"
                (-> entity-type
                    name
                    (str/replace #"-" "_")))]
    (fn [t]
      (import-local-file path)
      (t))))

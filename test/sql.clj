(ns sql
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.dal :as dal]
            [next.jdbc :as jdbc]))

(defn test-unique-constraint
  (edd/with-db-con
    (fn [ctx]
      (is
       (= {:error "bla"}
          (dal/datafy
           #(jdbc/execute! (:con ctx)
                           ["INSERT INTO glms.application_svc_seq(aggregate_id) VALUES ('1')"])))))))

(defn test-unique-constraint
  (edd/with-db-con
    (fn [ctx]
      (is
       (= {:error "bla"}
          (dal/datafy
           #((jdbc/execute! (:con ctx)
                            ["INSERT INTO glms.identity_store(service, aggregate_id, id)
                                    VALUES ('test-svc', '0b7a9436-58b1-11ea-82b4-0242ac130003', '1')"])
             (jdbc/execute! (:con ctx)
                            ["INSERT INTO glms.identity_store(service, aggregate_id, id)
                                    VALUES ('test-svc', '0b7a9436-58b1-11ea-82b4-0242ac130003', '1')"]))))))))

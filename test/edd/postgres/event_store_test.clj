(ns edd.postgres.event-store-test
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as dal]))

(def msg1 "ERROR: duplicate key value violates unique constraint \\\"event_store_pkey\\\"  Detail: Key (service, aggregate_id, event_seq)=(glms-application-svc, e188ae19-efa6-41c3-bf6c-8dfd9a280c1b, 6) already exists.")

(def msg2 "duplicate key value violates unique constraint \"part_event_store_3_pkey")

(def msg3 "ERROR: duplicate key value violates unique constraint \"part_identity_store_13_pkey\"  Detail: Key (aggregate_id, id)=(7315df44-ea35-42d9-aafb-4c0e979528aa, 7315df44-ea35-42d9-aafb-4c0e979528aa) already exists")

(deftest test-postgress-error-parsing
  (is (= {:key              :concurrent-modification
          :original-message msg1}
         (dal/parse-error msg1)))
  (is (= {:key              :concurrent-modification
          :original-message msg2}
         (dal/parse-error msg2)))
  (is (= {:key              :concurrent-modification
          :original-message msg3}
         (dal/parse-error msg3))))
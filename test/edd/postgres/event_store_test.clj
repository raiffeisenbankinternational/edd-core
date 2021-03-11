(ns edd.postgres.event-store-test
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as dal]))

(def msg1 "ERROR: duplicate key value violates unique constraint \\\"event_store_pkey\\\"  Detail: Key (service, aggregate_id, event_seq)=(glms-application-svc, e188ae19-efa6-41c3-bf6c-8dfd9a280c1b, 6) already exists.")

(deftest test-postgress-error-parsing
  (is (= {:key              :concurrent-modification
          :original-message msg1}
         (dal/parse-error msg1))))
(ns edd.dal-it
  (:require [clojure.test :refer [deftest is]]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [edd.dal :as dal]
            [edd.memory.event-store :as memory]
            [edd.postgres.event-store :as postgres]
            [edd.dynamodb.event-store :as dynamodb]
            [edd.core :as edd]
            [edd.common :as common]))

(defn get-ctx
  [service-name]
  {:service-name service-name
   :request-id (uuid/gen)
   :interaction-id (uuid/gen)
   :invocation-id (uuid/gen)
   :meta {:realm :test}
   :ref-date (util/date-time)
   :environment-name-lower "pipeline"
   :elastic-search {:scheme (util/get-env "IndexDomainScheme" "https")
                    :url (util/get-env "IndexDomainEndpoint")}
   :db {:endpoint (util/get-env "DatabaseEndpoint")
        :port "5432"
        :name "dynamodb-svc"
        :password (util/get-env "DatabasePassword" "no-secret")}
   :aws {:account-id (util/get-env "AccountId")
         :region (util/get-env "AWS_DEFAULT_REGION")
         :aws-access-key-id (util/get-env "AWS_ACCESS_KEY_ID")
         :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
         :aws-session-token (util/get-env "AWS_SESSION_TOKEN")}})

(def agg-id (uuid/gen))
(def agg-id-2 (uuid/gen))

(defn run-test
  [test-fn]
  (let [svc (str (uuid/gen))
        ctx (get-ctx svc)]
    (log/info "Svc" svc)
    (edd/with-stores
      (postgres/register ctx)
      test-fn)
    (edd/with-stores
      (memory/register ctx)
      test-fn)
    (edd/with-stores
      (dynamodb/register ctx)
      test-fn)))

(deftest test-simple-event
  (let [events [{:event-id :e1
                 :event-seq 1
                 :id agg-id}
                {:event-id :e2
                 :event-seq 2
                 :id agg-id}]
        events-2 [{:event-id :e-2-1
                   :event-seq 1
                   :id agg-id-2}]]

    (run-test
     #(do
        (dal/store-results (assoc %
                                  :resp {:events (concat events
                                                         events-2)
                                         :commands []
                                         :sequences []
                                         :identities []}))
        (is (= events
               (dal/get-events (assoc % :id agg-id))))
        (is (= 2
               (dal/get-max-event-seq (assoc % :id agg-id))))
        (is (= 1
               (dal/get-max-event-seq (assoc % :id agg-id-2))))))))

(deftest test-identities
  (let [id-1 (str "id-" (uuid/gen))
        identity {:identity id-1
                  :id agg-id}
        id-2 (str "id-2-" (uuid/gen))
        identity-2 {:identity id-2
                    :id agg-id-2}]
    (run-test
     #(do
        (dal/store-results (assoc %
                                  :resp {:events []
                                         :commands []
                                         :sequences []
                                         :identities [identity identity-2]}))
        (is (= agg-id
               (dal/get-aggregate-id-by-identity (assoc % :identity id-1))))
        (is (= agg-id-2
               (dal/get-aggregate-id-by-identity (assoc % :identity id-2))))
        (is (= nil
               (dal/get-aggregate-id-by-identity (assoc % :identity "agg-id-3"))))))))

(deftest test-sequence-number
  (let [id-1 (str "id-" (uuid/gen))
        identity {:identity id-1
                  :id agg-id}
        id-2 (str "id-2-" (uuid/gen))
        identity-2 {:identity id-2
                    :id agg-id-2}
        sequence-1 {:sequence :seq
                    :id agg-id}
        sequence-2 {:sequence :seq
                    :id agg-id-2}]
    (run-test
     #(do
        (dal/store-results (assoc %
                                  :resp {:events []
                                         :commands []
                                         :sequences [sequence-1 sequence-2]
                                         :identities [identity identity-2]}))
        (is (= agg-id
               (dal/get-aggregate-id-by-identity (assoc % :identity id-1))))
        (is (= agg-id-2
               (dal/get-aggregate-id-by-identity (assoc % :identity id-2))))
        (is (= nil
               (dal/get-aggregate-id-by-identity (assoc % :identity "agg-id-3"))))
        (is (= 1
               (common/get-sequence-number-for-id % agg-id)))))))

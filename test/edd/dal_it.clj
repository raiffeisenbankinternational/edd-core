(ns edd.dal-it
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [edd.dal :as dal]
            [edd.memory.event-store :as memory]
            [edd.postgres.event-store :as postgres]
            [edd.dynamodb.event-store :as dynamodb]
            [lambda.test.fixture.state :as state]
            [edd.core :as edd]))

(defn get-ctx
  []
  {:service-name           "dynamodb-svc"
   :request-id             (uuid/gen)
   :interaction-id         (uuid/gen)
   :invocation-id          (uuid/gen)
   :environment-name-lower "pipeline"
   :elastic-search         {:url (util/get-env "IndexDomainEndpoint")}
   :db                     {:endpoint (util/get-env "DatabaseEndpoint")
                            :port     "5432"
                            :name     "dynamodb-svc"
                            :password (util/get-env "DatabasePassword")}
   :aws                    {:region                (util/get-env "AWS_DEFAULT_REGION")
                            :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                            :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                            :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}})

(def agg-id (uuid/gen))
(def agg-id-2 (uuid/gen))

(defn run-test
  [test-fn]
  (let [ctx (get-ctx)]
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
  (let [events [{:event-id  :e1
                 :event-seq 1
                 :id        agg-id}
                {:event-id  :e2
                 :event-seq 2
                 :id        agg-id}]
        events-2 [{:event-id  :e-2-1
                   :event-seq 1
                   :id        agg-id-2}]]
    (run-test
     #(do
        (dal/store-results (assoc %
                                  :resp {:events     (concat events
                                                             events-2)
                                         :commands   []
                                         :sequences  []
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
                  :id       agg-id}
        id-2 (str "id-2-" (uuid/gen))
        identity-2 {:identity id-2
                    :id       agg-id-2}]
    (run-test
     #(do
        (dal/store-results (assoc %
                                  :resp {:events     []
                                         :commands   []
                                         :sequences  []
                                         :identities [identity identity-2]}))
        (is (= agg-id
               (dal/get-aggregate-id-by-identity (assoc % :identity id-1))))
        (is (= agg-id-2
               (dal/get-aggregate-id-by-identity (assoc % :identity id-2))))
        (is (= nil
               (dal/get-aggregate-id-by-identity (assoc % :identity "agg-id-3"))))))))

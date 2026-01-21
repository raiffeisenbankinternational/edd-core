(ns aws.dynamodb-it
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [aws.dynamodb :as ddb]
            [clojure.string :as str]
            [edd.dal :as dal]
            [edd.dynamodb.event-store :as event-store]
            [lambda.uuid :as uuid]))

(def request-id (uuid/gen))
(def interaction-id (uuid/gen))
(def invocation-id (uuid/gen))

(def ctx
  (-> {:elastic-search         {:scheme (util/get-env "IndexDomainScheme" "https")
                                :url (util/get-env "IndexDomainEndpoint")}
       :db                     {:name (util/get-env "ApplicationName" "dynamodb-svc")}
       :request-id             request-id
       :interaction-id         interaction-id
       :invocation-id          invocation-id
       :service-name           "test-source"
       :meta                   {:realm :test}
       :environment-name-lower (util/get-env "EnvironmentNameLower")
       :aws                    {:region                (util/get-env "AWS_DEFAULT_REGION")
                                :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                                :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                                :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}}
      (event-store/register)))

(deftest test-list-tables
  (is (= (sort ["effect-store-ddb"
                "event-store-ddb"
                "identity-store-ddb"
                "request-log-ddb"
                "response-log-ddb"])
         (->> (ddb/list-tables ctx)
              (:TableNames)
              (filter #(str/includes? % "dynamodb-svc"))
              (map #(str/replace % #".*-svc-test-" ""))
              sort))))

(deftest store-results-test
  (let [agg-id (uuid/gen)
        identity-id (str "id-" (uuid/gen))
        identity {:identity identity-id
                  :id       agg-id}
        event {:event-id  :e1
               :event-seq 1
               :id        agg-id}
        effect {:service  :test-svc
                :commands [{:cmd-id :cmd-test
                            :id     agg-id}]}
        ;; Effect ID is created from request-id and breadcrumbs, not uuid/gen
        effect-id (str request-id "-0")]
    (dal/store-results (assoc ctx
                              :resp {:events     [event]
                                     :effects    [effect]
                                     :identities [identity]}))

    (is (= {:Item {:Data          {:S (util/to-json event)}
                   :EventSeq      {:N "1"}
                   :AggregateId   {:S agg-id}
                   :InteractionId {:S interaction-id}
                   :ItemType      {:S :event}
                   :RequestId     {:S request-id}
                   :InvocationId  {:S invocation-id}
                   :Breadcrumbs   {:S "0"}
                   :Service       {:S :test-source}}}
           (ddb/make-request
            (assoc ctx :action "GetItem"
                   :body {:Key       {:AggregateId {:S agg-id}
                                      :EventSeq    {:N "1"}}
                          :TableName (event-store/table-name ctx :event-store)}))))
    (let [effect-result (ddb/make-request
                         (assoc ctx :action "GetItem"
                                :body {:Key       {:Id {:S effect-id}}
                                       :TableName (event-store/table-name ctx :effect-store)}))
          expected-data (util/to-json (assoc effect
                                             :request-id request-id
                                             :interaction-id interaction-id))]
      ;; Verify each field individually to avoid map ordering issues
      (is (= expected-data (get-in effect-result [:Item :Data :S])) "Effect Data matches")
      (is (= effect-id (get-in effect-result [:Item :Id :S])) "Effect Id matches")
      (is (= interaction-id (get-in effect-result [:Item :InteractionId :S])) "InteractionId matches")
      (is (= :effect (get-in effect-result [:Item :ItemType :S])) "ItemType matches")
      (is (= request-id (get-in effect-result [:Item :RequestId :S])) "RequestId matches")
      (is (= invocation-id (get-in effect-result [:Item :InvocationId :S])) "InvocationId matches")
      (is (= "0" (get-in effect-result [:Item :Breadcrumbs :S])) "Breadcrumbs matches")
      (is (= :test-svc (get-in effect-result [:Item :TargetService :S])) "TargetService matches")
      (is (= :test-source (get-in effect-result [:Item :Service :S])) "Service matches"))
    (is (= {:Item {:Data          {:S (util/to-json identity)}
                   :AggregateId   {:S agg-id}
                   :Id            {:S (str "test-source/" identity-id)}
                   :InteractionId {:S interaction-id}
                   :ItemType      {:S :identity}
                   :RequestId     {:S request-id}
                   :InvocationId  {:S invocation-id}
                   :Breadcrumbs   {:S "0"}
                   :Service       {:S :test-source}}}
           (ddb/make-request
            (assoc ctx :action "GetItem"
                   :body {:Key       {:Id {:S (str "test-source/" identity-id)}}
                          :TableName (event-store/table-name ctx :identity-store)}))))))

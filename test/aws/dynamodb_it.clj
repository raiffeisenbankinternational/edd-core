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

(def ctx
  (-> {:elastic-search         {:url (util/get-env "IndexDomainEndpoint")}
       :db                     {:name "dynamodb-svc"}
       :request-id             request-id
       :interaction-id         interaction-id
       :service-name           "test-source"
       :environment-name-lower "pipeline"
       :aws                    {:region                (util/get-env "AWS_DEFAULT_REGION")
                                :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                                :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                                :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}}
      (event-store/register)))

(deftest test-list-tables
  (is (= ["effect-store-ddb"
          "event-store-ddb"
          "identity-store-ddb"]
         (->> (ddb/list-tables ctx)
              (:TableNames)
              (filter #(str/includes? % "dynamodb-svc"))
              (map #(str/replace % #".*-svc-" ""))))))

(def agg-id (uuid/gen))
(def effect-id (uuid/gen))

(deftest store-results-test
  (let [identity {:identity "id-1"
                  :id       agg-id}
        event {:event-id  :e1
               :event-seq 1
               :id        agg-id}
        command {:service  :test-svc
                 :commands [{:cmd-id :cmd-test
                             :id     agg-id}]}]
    (with-redefs [uuid/gen (fn [] effect-id)]
      (dal/store-results (assoc ctx
                                :resp {:events     [event]
                                       :commands   [command]
                                       :sequences  []
                                       :identities [identity]})))

    (is (= {:Item {:Data          {:S (util/to-json event)}
                   :EventSeq      {:N "1"}
                   :Id            {:S agg-id}
                   :InteractionId {:S interaction-id}
                   :ItemType      {:S :event}
                   :RequestId     {:S request-id}
                   :Service       {:S :test-source}}}
           (ddb/make-request
            (assoc ctx :action "GetItem"
                   :body {:Key       {:Id       {:S agg-id}
                                      :EventSeq {:N "1"}}
                          :TableName (event-store/table-name ctx :event-store)}))))
    (is (= {:Item {:Data          {:S (util/to-json (assoc command
                                                           :request-id request-id
                                                           :interaction-id interaction-id))}
                   :Id            {:S effect-id}
                   :InteractionId {:S interaction-id}
                   :ItemType      {:S :effect}
                   :RequestId     {:S request-id}
                   :TargetService {:S :test-svc}
                   :Service       {:S :test-source}}}
           (ddb/make-request
            (assoc ctx :action "GetItem"
                   :body {:Key       {:Id {:S effect-id}}
                          :TableName (event-store/table-name ctx :effect-store)}))))
    (is (= {:Item {:Data          {:S (util/to-json identity)}
                   :AggregateId   {:S agg-id}
                   :Id            {:S "test-source/id-1"}
                   :InteractionId {:S interaction-id}
                   :ItemType      {:S :identity}
                   :RequestId     {:S request-id}
                   :Service       {:S :test-source}}}
           (ddb/make-request
            (assoc ctx :action "GetItem"
                   :body {:Key       {:Id {:S "test-source/id-1"}}
                          :TableName (event-store/table-name ctx :identity-store)}))))))

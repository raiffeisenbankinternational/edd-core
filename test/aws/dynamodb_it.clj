(ns aws.dynamodb-it
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [aws.dynamodb :as ddb]))

(def ctx
  {:elastic-search {:url (util/get-env "IndexDomainEndpoint")}
   :aws            {:region                (util/get-env "AWS_DEFAULT_REGION")
                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}})

(deftest test-list-tables
  (is (= {:TableNames ["pipeline-alpha-dynamodb-svc-effects-store-ddb"
                       "pipeline-alpha-dynamodb-svc-event-store-ddb"
                       "pipeline-alpha-dynamodb-svc-identity-store-ddb"]}
         (ddb/list-tables ctx))))
(ns sdk.aws.repl
  (:require [clojure.test :refer :all]
            [sdk.aws.cognito-idp :as cognito-idp]
            [lambda.util :as util]))

(deftest test1
  (is (= {}
         (cognito-idp/admin-get-user
          {:aws  {:aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                  :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                  :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}
           :auth {:user-pool-id "eu-central-1_tk6YrMrPc"}}
          {:username "PingFederate_robert.pofuk@rbinternational.com"}))))

(deftest test2
  (is (= {}
         (cognito-idp/admin-list-groups-for-user
          {:aws  {:aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                  :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                  :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}
           :auth {:user-pool-id "eu-central-1_tk6YrMrPc"}}
          {:username "PingFederate_robert.pofuk@rbinternational.com"}))))


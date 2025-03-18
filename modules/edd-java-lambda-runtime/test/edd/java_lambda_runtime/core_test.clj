(ns edd.java-lambda-runtime.core-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.tools.logging :as log]
   [edd.core :refer [reg-cmd]]
   [edd.java-lambda-runtime.core :as sut]
   [edd.memory.event-store :as memory-event-store]
   [edd.memory.view-store :as memory-view-store]
   [lambda.test.fixture.core :refer [mock-core]]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [malli.core :as m]
   [sdk.aws.common :as common]
   [sdk.aws.sqs :as sqs])
  (:import
   [java.io ByteArrayInputStream StringWriter]))

(def mock-ctx
  (-> {}
      (reg-cmd :test-native-cmd (fn [_ _]
                                  (log/info "Native test command")
                                  {:result {:status :ok}})
               :consumes  (m/schema
                           [:map {:closed true}
                            [:cmd-id [:= :test-native-cmd]]
                            [:id uuid?]]))
      (memory-view-store/register)
      (memory-event-store/register)))

(sut/register-java-runtime! mock-ctx)

(deftest edd-java-lambda-runtime-core
  (testing "Java Runtime should be compatible with edd-core"
    #_{:clj-kondo/ignore [:unresolved-symbol]}
    (let [messages (atom [])]
      (with-redefs [sut/metrics-initialized? (volatile! true)
                    common/create-date (fn [] "20210322T232540Z")
                    sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                      (swap! messages #(conj % message)))]
        (mock-core
         :invocations []
         :requests [{:put  "https://s3.eu-central-1.amazonaws.com/local-local-sqs/response/7b202505-ea6f-45d0-819d-9fd961b3c2b0/0/glms-consolidation-svc.json"
                     :status 200
                     :body (char-array "OK")}]
         (let [cmd-id (uuid/gen)
               request-id "7b202505-ea6f-45d0-819d-9fd961b3c2b0"
               ctx  (reify com.amazonaws.services.lambda.runtime.Context
                      (getFunctionName [_] "test-function")
                      (getFunctionVersion [_] "1.0")
                      (getInvokedFunctionArn [_] "arn:aws:lambda:us-east-1:7b202505-ea6f-45d0-819d-9fd961b3c2b0:function:test-function")
                      (getMemoryLimitInMB [_] 128)
                      (getRemainingTimeInMillis [_] 1000)
                      (getAwsRequestId [_] request-id)
                      (getLogGroupName [_] "/aws/lambda/test-function")
                      (getLogStreamName [_] "2020/01/01/[$LATEST]7b202505-ea6f-45d0-819d-9fd961b3c2b0")
                      (getClientContext [_] nil)
                      (getIdentity [_] nil))
               payload {:request-id (uuid/parse request-id)
                        :meta {:realm :test}
                        :commands [{:cmd-id :test-native-cmd :id cmd-id}]}
               sb (ByteArrayInputStream. (.getBytes (util/to-json payload) "UTF-8"))
               sw (StringWriter.)]
           (#'-handleRequest nil sb sw ctx)
           (is (= (uuid/parse request-id) (second (first (util/to-edn (str sw)))))))
         nil)))))
(ns e2e.api-test
  "Integration tests against the deployed E2E stack (real API Gateway + Lambdas +
   DynamoDB + S3 + SQS). The deploy is done by e2e/run-e2e.sh, which exports:
     ApiUrl               - base URL of the deployed REST API
     E2E_IMPORT_OBJECT_ID - aggregate id of the file uploaded to the import bucket

   HTTP + JSON go through edd-core's own helpers so the wire format (\":kw\" /
   \"#uuid\" encoding) matches the services exactly."
  (:require [clojure.test :refer [deftest is testing]]
            [lambda.util :as util]
            [lambda.uuid :as uuid]))

(def api-url
  (util/get-env "ApiUrl"))

(def realm :test)

(defn post
  [path payload]
  (util/http-post (str api-url path)
                  {:body (util/to-json payload)
                   :timeout 30000
                   :headers {"Content-Type" "application/json"}}))

(defn cmd!
  [svc cmd-id id attrs & {:keys [request-id version]}]
  (post (str "/" svc "/commands")
        {:request-id (or request-id (uuid/gen))
         :interaction-id (uuid/gen)
         :meta {:realm realm}
         :commands [(cond-> {:cmd-id cmd-id
                             :id id
                             :attrs attrs}
                      version (assoc :version version))]}))

(defn query
  [svc q]
  (post (str "/" svc "/query")
        {:request-id (uuid/gen)
         :interaction-id (uuid/gen)
         :meta {:realm realm}
         :query q}))

(defn aggregate
  [svc id]
  (-> (query svc {:query-id :get-by-id
                  :id id})
      :body
      :result))

(defn success?
  [resp]
  (and (= 200 (:status resp))
       (true? (get-in resp [:body :result :success]))))

(defn wait-for
  "Polls (f) until (pred result) is truthy (tolerates DynamoDB read-after-write
   lag and the async router round-trips). Returns the last result."
  [pred f]
  (loop [n 20]
    (let [v (f)]
      (if (or (pred v) (zero? n))
        v
        (do (Thread/sleep 2000)
            (recur (dec n)))))))

(deftest ping-pong-effect-loop
  (testing "a :ping command bounces ping<->pong via the router and stops at the hop guard"
    (let [id
          (uuid/gen)

          resp
          (cmd! "ping-svc" :ping id {:hops 0})]

      (is
       (success? resp))

      (let [agg
            (wait-for #(= 5 (:hops %))
                      #(aggregate "pong-svc" id))]

        (is
         (= 5 (:hops agg)))))))

(deftest s3-bucket-filter
  (testing "a file uploaded to the import bucket becomes an :object-uploaded command"
    (let [obj-id
          (uuid/parse (util/get-env "E2E_IMPORT_OBJECT_ID"))

          agg
          (wait-for #(= :object-recorded (:last %))
                    #(aggregate "ping-svc" obj-id))]

      (is
       (= :object-recorded (:last agg))))))

(deftest cross-service-remote-dependency
  (testing "pong's :combine depends (over the API) on ping's aggregate value+version"
    (let [ping-id
          (uuid/gen)

          pong-id
          (uuid/gen)]

      (is
       (success? (cmd! "ping-svc" :set-value ping-id {:value "ping-v1"})))

      (is
       (= 1 (:version (wait-for #(= 1 (:version %))
                                #(aggregate "ping-svc" ping-id)))))

      (cmd! "pong-svc" :combine pong-id {:ping-id ping-id :value "pong-v1"})

      (let [agg
            (wait-for #(= 1 (:version %))
                      #(aggregate "pong-svc" pong-id))]

        (is
         (= "ping-v1" (:ping-value agg)))

        (is
         (= 1 (:ping-version agg)))

        (is
         (= "pong-v1" (:pong-value agg))))

      (testing "updating ping bumps its version and pong's dependency reflects it"
        (is
         (success? (cmd! "ping-svc" :set-value ping-id {:value "ping-v2"})))

        (is
         (= 2 (:version (wait-for #(= 2 (:version %))
                                  #(aggregate "ping-svc" ping-id)))))

        (cmd! "pong-svc" :combine pong-id {:ping-id ping-id :value "pong-v2"})

        (let [agg
              (wait-for #(= 2 (:version %))
                        #(aggregate "pong-svc" pong-id))]

          (is
           (= "ping-v2" (:ping-value agg)))

          (is
           (= 2 (:ping-version agg)))

          (is
           (= "pong-v2" (:pong-value agg))))))))

(deftest identity-uniqueness
  (let [a1
        (uuid/gen)

        a2
        (uuid/gen)

        name
        (str "alice-" (uuid/gen))]

    (testing "first aggregate claims the name"
      (is
       (success? (cmd! "ping-svc" :claim-name a1 {:name name}))))

    (testing "a second aggregate claiming the same name is rejected with a clean :identity-conflict (not the raw DynamoDB exception)"
      (let [dup
            (cmd! "ping-svc" :claim-name a2 {:name name})]

        (is
         (= 500 (:status dup)))

        (is
         (= :identity-conflict (get-in dup [:body :exception :key])))))

    (testing "the second aggregate can claim a different name"
      (is
       (success? (cmd! "ping-svc" :claim-name a2 {:name (str name "-2")}))))))

(deftest command-validation
  (let [id
        (uuid/gen)]

    (testing "a valid command succeeds"
      (is
       (success? (cmd! "ping-svc" :set-score id {:score 42}))))

    (testing "a value failing the :consumes schema returns a structured error"
      (let [bad
            (cmd! "ping-svc" :set-score id {:score -5})]

        (is
         (= 500 (:status bad)))

        (is
         (some? (get-in bad [:body :error])))))

    (testing "a missing required attr returns a structured error"
      (let [missing
            (cmd! "ping-svc" :set-score id {})]

        (is
         (= 500 (:status missing)))

        (is
         (some? (get-in missing [:body :error])))))))

(deftest idempotency
  (let [id
        (uuid/gen)

        req
        (uuid/gen)]

    (testing "first command with the request-id is applied"
      (is
       (success? (cmd! "ping-svc" :set-value id {:value "once"} :request-id req)))

      (is
       (= 1 (:version (wait-for #(= 1 (:version %))
                                #(aggregate "ping-svc" id))))))

    (testing "replaying the same request-id is deduplicated (no second event)"
      (cmd! "ping-svc" :set-value id {:value "twice"} :request-id req)
      (Thread/sleep 3000)
      (let [agg
            (aggregate "ping-svc" id)]

        (is
         (= 1 (:version agg)))

        (is
         (= "once" (:value agg)))))))

(deftest concurrent-modification
  (testing "a command :version that doesn't match the aggregate's current version is rejected"
    (let [id
          (uuid/gen)]

      (is
       (success? (cmd! "ping-svc" :set-value id {:value "v1"})))

      (is
       (= 1 (:version (wait-for #(= 1 (:version %))
                                #(aggregate "ping-svc" id)))))

      (testing "matching version succeeds and bumps the version"
        (is
         (success? (cmd! "ping-svc" :set-value id {:value "v2"} :version 1)))

        (is
         (= 2 (:version (wait-for #(= 2 (:version %))
                                  #(aggregate "ping-svc" id))))))

      (testing "stale version is rejected with a concurrent-modification error"
        (let [stale
              (cmd! "ping-svc" :set-value id {:value "v3"} :version 1)]

          (is
           (= 500 (:status stale)))

          (is
           (= :concurrent-modification (get-in stale [:body :exception])))))

      (testing "rejected command left the aggregate untouched"
        (let [agg
              (aggregate "ping-svc" id)]

          (is
           (= 2 (:version agg)))

          (is
           (= "v2" (:value agg)))))

      (testing "command with the current version succeeds again"
        (is
         (success? (cmd! "ping-svc" :set-value id {:value "v3"} :version 2)))))))

(deftest multi-service-fan-out
  (testing "one event fans effects out to both ping-svc and pong-svc via the router"
    (let [b
          (uuid/gen)

          ping-target
          (uuid/gen)

          pong-target
          (uuid/gen)]

      (is
       (success? (cmd! "ping-svc" :broadcast b {:ping-target ping-target
                                                :pong-target pong-target
                                                :value "fanned"})))

      (is
       (= "fanned" (:value (wait-for #(= "fanned" (:value %))
                                     #(aggregate "ping-svc" ping-target)))))

      (is
       (= "fanned" (:value (wait-for #(= "fanned" (:value %))
                                     #(aggregate "pong-svc" pong-target))))))))

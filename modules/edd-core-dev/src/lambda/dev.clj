(ns lambda.dev
  (:require [aws.aws :as aws]
            [clojure.core.async :as async :refer [>!! go-loop]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure.core :as compojure :refer [POST]]
            [compojure.route :as route]
            [ring.middleware.cors :refer [wrap-cors]]
            [edd.postgres.event-store :as postgres]
            [jsonista.core :as json]
            [aws.lambda :as lambda]
            [lambda.jwt :as jwt]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [ring.adapter.jetty9 :as jetty]))

(declare default-response-handler)

;; --- queues

(defonce queues
  (atom {}))

(defn register-queue! [queue-name]
  (swap! queues assoc queue-name clojure.lang.PersistentQueue/EMPTY))

(defn ensure-queue! [queue-name]
  (when-not (queue-name @queues)
    (register-queue! queue-name)))

(defn queue-message! [queue-name message]
  (ensure-queue! queue-name)
  (swap! queues update queue-name (fn [q]
                                    (conj q message))))

(defn peek-message [queue-name]
  (peek (get @queues queue-name)))

(defn pop-message! [queue-name]
  (swap! queues update queue-name (fn [q]
                                    (pop q))))

(defn purge-queues! []
  (reset! queues {}))

(comment
  (register-queue! :limit-lifecycle-svc)
  (queue-message! :limit-lifecycle-svc {:fu :bar})
  (queue-message! :limit-lifecycle-svc {:a :b})
  (peek-message :limit-lifecycle-svc)
  (pop-message! :limit-lifecycle-svc)
  (peek-message :limit-lifecycle-svc))

;; --- lambda lifecycle

(defn- parse-token-redef
  [ctx _]
  (assoc ctx :user {:id "email"
                    :email "email"
                    :roles [:realm-test :role]
                    :department_code "000"
                    :department "No Department"}))

(defn- send-response-redef
  [response-handler message]
  (fn [{:keys [resp] :as ctx}]
    (if response-handler
      (response-handler resp message)
      (default-response-handler resp message))))

(defn- get-env-redef
  [config-load]
  (fn [var-name & [default]]
    ;; NOTE: we load config on every request because some vars can expire
    ;; does not matter from performance perspective, since it is for dev purposes only
    (let [config (config-load)]
      (or (get config var-name)
          (do
            (log/warn (str "returning default value: " default " for ENV var " var-name) ::ns)
            default)))))

(defn- get-next-invocation-redef
  [config-load cmd-id query-id message]
  (fn [_]
    ;; NOTE: here we inject the message with command / query into the lambda workflow
    (log/info "received request" message)
    {:headers {:lambda-runtime-aws-request-id (str (uuid/gen))}
     :body    {:headers {:x-authorization (get (config-load) "id-token")}
               :path (if query-id "/query" "/command")
               :attributes {:ApproximateReceiveCount "1"}
               :body (util/to-json (cond-> {:request-id     (uuid/gen)
                                            :interaction-id (uuid/gen)
                                            :user {:selected-role :role}}
                                     cmd-id   (assoc :commands       [message])
                                     query-id (assoc :query message)))}}))

(defn- store-command-redef
  [ctx {:keys [service commands] :as cmd}]
  ;; NOTE: here we inject effects
  (log/info "storing effect" cmd)
  (postgres/store-cmd ctx (assoc
                           cmd
                           :request-id (:request-id ctx)
                           :interaction-id (:interaction-id ctx)))
  (doall
   (for [command commands]
     ;; queue all the effects in the respective queue for that service
     (queue-message! service command))))

(defn start* [{:keys [service-name config-load entrypoint response-handler]
               :as   args}]
  (log/info (str "Starting " service-name) args)
  (go-loop []
    (let [next           (peek-message service-name)
          handle-message (fn [{:keys [cmd-id query-id]
                               :as   message}]
                           (log/info "handling message" message)
                           (with-redefs [jwt/parse-token parse-token-redef
                                         aws/get-token (fn [_] (get (config-load) "id-token"))
                                         util/load-config (fn [_] {})
                                         lambda/get-loop (fn [] [0])
                                         lambda/send-response (send-response-redef response-handler message)
                                         util/get-env (get-env-redef config-load)
                                         aws/get-next-invocation (get-next-invocation-redef config-load cmd-id query-id message)
                                         postgres/store-command store-command-redef]
                             ;; NOTE : usually `-main`
                             (entrypoint)))]
      (cond
        (nil? next)
        (do
          ;; If the queue is empty, wait for 2 seconds and poll again
          ;; (log/info "Polling...")
          (Thread/sleep 2000)
          (recur))

        (= :shutdown next)
        (do (pop-message! service-name)
            (log/info (str service-name " is shutting down ...") ::ns))

        ;; handle the message and repeat
        :else (do (pop-message! service-name)
                  (handle-message next)
                  (recur))))))

(defn stop* [service-name]
  (queue-message! service-name :shutdown))

(defn load-config [path]
  (try
    (-> (io/resource path)
        slurp
        edn/read-string)
    (catch Exception _
      ;; ignore any errors, like missing file
      )))

(defmacro deflambda
  "Example:

  `(deflambda glms-limit-lifecycle-svc
     :entrypoint main/-main
     :config (dev/load-config \"config.edn\")
     :response-handler prn)`

  Where `main/-man` is an entry point to the lambda service and `config.edn` is a file with runtime secrets (it needs to be on the classpath, e.g in `resources` directory).
  Alternatively you can pass `config` as a function which always returns a map:

  {\"IndexDomainEndpoint\"   \"vpc-dev16-glms-index-svc-es-v9-4w5uux57ttvsqf6pvetl2jg4rm.eu-central-1.es.amazonaws.com\"
   \"DatabaseEndpoint\"      \"dev16-glms-health-db-v11.c0srqkcf6umv.eu-central-1.rds.amazonaws.com\"
   \"EnvironmentNameLower\"  \"dev16\"
   \"ServiceName\"           \"glms-limit-lifecycle-svc\"
   \"Region\"                \"eu-central-1\"
   \"AccountId\"             \"419951837664\"
   \"AWS_ACCESS_KEY_ID\"     \"PLACEHOLDER\"
   \"AWS_SECRET_ACCESS_KEY\" \"PLACEHOLDER\"
   \"AWS_SESSION_TOKEN\"     \"PLACEHOLDER\"
   \"DatabasePassword\"      \"PLACEHOLDER\"}

  NOTE: `AWS_SESSION_TOKEN` has an expiry date, after which you will likely need to obtain another set of AWS credentials.

  `deflambda` declares two functions in the current namespace: `start` and `stop`.
  Calling (start) will create a `go` thread that periodically polls the queue for messages.
  You can queue a command or a query on it like so:

    (queue-message! :glms-limit-lifecycle-svc {:cmd-id :create-limit :id (uuid/gen)})
    (queue-message! :glms-limit-lifecycle-svc {:query-id :get-current-limit-status :limit-id #uuid \"2d28bac1-757f-4205-a9d8-15b46e6d4ca4\"})

  Calling (stop) puts a special message on the queue that will shut down the thread when processed."
  [service-name & params]
  (let [{:keys [config entrypoint response-handler]} (apply hash-map params)]
    `(do
       (defn ~(symbol 'start) []
         (start*  {:service-name     ~(keyword service-name)
                   :config-load      ~config
                   :entrypoint       ~entrypoint
                   :response-handler ~response-handler}))
       (defn ~(symbol 'stop) []
         (stop* ~(keyword service-name))))))

;; --- api-gateway

(defonce gateway (atom nil))

(def pub-chan (async/chan 1))
(def publisher (async/pub pub-chan :topic))
(def sub-chan (async/chan))

(defn decode-body [body]
  (let [body (-> body
                 slurp
                 (json/read-value json/keyword-keys-object-mapper))]
    (cond-> body
      (:query-id body) (update :query-id keyword)
      (:cmd-id body)   (update :cmd-id keyword))))

(defn default-response-handler [response request]
  (let [topic (hash request)]
    (log/info "publishing response" {:request request :topic topic :response response})
    ;; NOTE : publishes response to the topic which corresponds to hashed request map
    (>!! pub-chan {:topic topic :response response})
    nil))

(defn handle-request
  "async handler, will block the client http call until a response arrives."
  [request send-response raise-error]
  (let [{:keys [params body]} request
        service-name          (-> params :service keyword)
        message               (decode-body body)
        topic                 (hash message)]
    (let [_            (log/info "handling request" {:service/name service-name
                                                     :message      message
                                                     :topic        topic})
          _            (queue-message! service-name message)
          ;; NOTE : subscription is based on the hashed request
          subscription (async/sub publisher topic sub-chan)]
      (let [timeout                   (async/timeout 5000)
            [{:keys [response]} port] (async/alts!! [sub-chan timeout])]
        (if (= port sub-chan)
          (send-response
           {:status 200
            :body   (json/write-value-as-string response)})
          (send-response
           {:status 408
            :body   "Response Timeout"}))))))

(defn gateway-start* []
  (let [routes (-> (-> (compojure/routes
                        (POST "/:service/query" [] handle-request)
                        (POST "/:service/command" [] handle-request)
                        (route/not-found "Route not found")))
                   (wrap-cors :access-control-allow-origin [#".*"]
                              :access-control-allow-methods [:post]))]
    (jetty/run-jetty routes {:host   "127.0.0.1"
                             :port   3001
                             :async? true
                             :join?  false})))

(defn start-gateway!
  "Starts a local instance of api gateway.
  Accepts requests at two endpoints `/command/:service` and `/query/:service`.
  Example request (a query):

  curl -X POST http://127.0.0.1:3001/query/glms-limit-lifecycle-svc --data '{\"query-id\": \"get-current-limit-status\", \"limit-id\": \"2d28bac1-757f-4205-a9d8-15b46e6d4ca4\"}'

  Messages will be published to the respective queue for the given `:service`.
  The request handler creates a subscription that blocks the client and waits for a response.
  Request/response routing is based on a hash of the request."
  []
  (reset! gateway (gateway-start*)))

(defn stop-gateway! []
  (when @gateway
    (.stop @gateway))
  (reset! gateway nil))

(defn restart-gateway! []
  (stop-gateway!)
  (start-gateway!))

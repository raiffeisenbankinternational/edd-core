(ns lambda.dev
  (:require [aws :as aws]
            [clojure.core.async :as async :refer [go-loop]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [edd.postgres.event-store :as postgres]
            [lambda.core :as lambda]
            [lambda.util :as util]
            [lambda.uuid :as uuid]))

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

(defn start* [{:keys [service-name config-load entrypoint response-handler]
               :or   {response-handler (fn [response]
                                         (log/info (str service-name " response") response))}
               :as   args}]
  (log/info (str "Starting " service-name) args)
  (go-loop []
    (let [next           (peek-message service-name)
          handle-message (fn [{:keys [cmd-id query-id]
                               :as   message}]
                           (log/info "handling message" message)
                           (with-redefs [util/load-config     (fn [_] {})
                                         lambda/get-loop      (fn [] [0])
                                         lambda/send-response (fn [{:keys [resp] :as ctx}]
                                                                (response-handler resp))
                                         util/get-env         (fn [var-name & [default]]
                                                                ;; NOTE: we load config on every request because some vars can expire (
                                                                ;; does not matter from performance perspective, since it is for dev purposes only
                                                                (let [config (config-load)]
                                                                  (or (get config var-name)
                                                                      (do
                                                                        (log/warn (str "returning default value: " default " for ENV var " var-name) ::ns)
                                                                        default))))
                                         ;; NOTE: here we inject the message with command / query into the lambda workflow
                                         aws/get-next-request (fn [_]
                                                                (log/info "received request" message)
                                                                {:headers {:lambda-runtime-aws-request-id (str (uuid/gen))}
                                                                 :body    (cond-> {:request-id     (uuid/gen)
                                                                                   :interaction-id (uuid/gen)}
                                                                            cmd-id   (assoc :commands       [message])
                                                                            query-id (assoc :query message))})

                                         ;; NOTE: here we inject effects
                                         postgres/store-command (fn [ctx {:keys [service commands] :as cmd}]
                                                                  (log/info "storing effect" cmd)
                                                                  (postgres/store-cmd ctx (assoc
                                                                                           cmd
                                                                                           :request-id (:request-id ctx)
                                                                                           :interaction-id (:interaction-id ctx)))
                                                                  (doall
                                                                   (for [command commands]
                                                                      ;; queue all the effects in the respective queue for that service
                                                                     (queue-message! service command))))]
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
        (do
          (pop-message! service-name)
          (log/info (str service-name " is shutting down ...") ::ns))

        ;; handle the message and repeat
        :else (do
                (pop-message! service-name)
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

(comment
  (register-queue! :limit-lifecycle-svc)
  (queue-message! :limit-lifecycle-svc {:fu :bar})
  (queue-message! :limit-lifecycle-svc {:a :b})
  (peek-message :limit-lifecycle-svc)
  (pop-message! :limit-lifecycle-svc)
  (peek-message :limit-lifecycle-svc))

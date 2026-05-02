(ns edd.router.core
  (:require [clojure.tools.logging :as log]
            [lambda.util :as util]
            [edd.router.util :as r-util]
            [edd.router.s3-handler :as s3-handler]))

(defn distribute-messages
  [ctx {:keys [response]}]
  (let [resp (get response :resp)
        events (:events resp)
        effects (or (:commands resp)
                    (:effects resp))
        _ (log/info "Encountered commands: " (count effects)
                    ", events: " (count events))
        service (:service response)
        messages (into (r-util/map-effects ctx effects 0)
                       (r-util/map-events ctx service events (count effects)))
        resp (r-util/group-and-publish ctx messages)]
    (when (some #(not= (:success %) true) resp)
      (throw (ex-info "Some messages not processed"
                      {:exception resp})))
    {:success true}))

(defn handle-request
  [ctx record]
  (let [{:keys [s3] :as record} (if (contains? record :Records)
                                  (-> record :Records first)
                                  record)]
    (if s3
      (s3-handler/handle-messages ctx record)
      (distribute-messages ctx {:response record}))))

(defn handler
  "Lambda handler body. Consumes the router response queue ({env}-router-svc-response)
   and fans each effect/event out to the target service's command/event FIFO queue."
  [ctx body]
  (log/info "Handling data" body)
  (util/d-time
   "Processing request"
   (mapv
    (fn [msg]
      (try
        (handle-request ctx (util/to-edn (:body msg)))
        (catch Exception e
          (log/error "Processing failed" e)
          {:exception (or (ex-data e)
                          (.getMessage e)
                          "Unknown error")})))
    (:Records body))))

(ns edd.router.s3-handler
  (:require [clojure.tools.logging :as log]
            [lambda.util :as util]
            [edd.router.util :as r-util]
            [sdk.aws.s3 :as s3]))

(defn distribute-messages
  [ctx content]
  (let [service (:service content)
        content (get content :resp content)

        effects (or (:commands content)
                    (:effects content))
        effects (r-util/map-effects ctx effects 0)
        effects (reduce
                 (fn [p v]
                   (assoc p (:id v) v))
                 {}
                 effects)

        events (:events content)
        events (r-util/map-events ctx service events (count effects))
        events (reduce
                (fn [p v]
                  (assoc p (:id v) v))
                {}
                events)

        _ (log/info "Encountered s3 commands: "
                    (count effects)
                    ", events: "
                    (count events))

        events-resp (r-util/group-and-publish ctx (vals events))
        events-resp (reduce
                     (fn [p {:keys [success id]}]
                       (if-not success
                         (conj p (get-in events [id :source]))
                         p))
                     []
                     events-resp)
        effects-resp (r-util/group-and-publish ctx (vals effects))
        effects-resp (reduce
                      (fn [p {:keys [success id]}]
                        (if-not success
                          (conj p (get-in effects [id :source]))
                          p))
                      []
                      effects-resp)
        remaining (cond-> {}
                    (seq events-resp) (assoc :events events-resp)
                    (seq effects-resp) (assoc :effects effects-resp))]
    remaining))

(defn handle-messages
  [ctx object]
  (let [{:keys [error] :as content} (s3/get-object ctx object)
        _ (when error
            (throw (ex-info "Failed to fetch object" error)))

        {:keys [service] :as content} (-> content
                                          slurp
                                          util/to-edn)

        {:keys [events effects] :as remaining} (distribute-messages ctx content)]
    (if-not (seq remaining)
      {:success true}
      (do
        (log/error "Not all messages distributed")
        (s3/put-object ctx (assoc-in object
                                     [:s3 :object :content]
                                     (util/to-json
                                      {:service service
                                       :resp remaining})))
        {:message "Not all messages distributed"
         :exception {:events (count events)
                     :effects (count effects)}}))))

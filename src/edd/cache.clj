(ns edd.cache
  (:require
   [edd.el.event :as event]
   [edd.dal :as dal]
   [lambda.uuid :as uuid]
   [lambda.request :as request]))

;; TODO
;; implement a functional approach
;; this code is NOT thread safe
;; this code is highly stateful


(defn events-with-version
  [last-event-seqs events]

  (loop [evts events
         versioned-events []
         new-versions last-event-seqs]
    (if (empty? evts)
      {:events         versioned-events
       :last-event-seq new-versions}
      (let [evt (first evts)
            nv (update new-versions (:id evt) (fnil inc 0))
            versioned-evt (assoc evt :event-seq (get nv (:id evt)))]
        (recur
         (rest evts)
         (conj versioned-events versioned-evt)
         nv)))))

(defn track-intermediate-events!
  [{:keys [resp] :as ctx}]
  (let [request (events-with-version
                 (get @request/*request* :last-event-seq {})
                 (:events resp))]

    (swap! request/*request* update-in [:events] (fnil concat []) (:events request))
    (swap! request/*request* assoc-in [:last-event-seq] (:last-event-seq request)))
  ctx)

(defn apply-events [{:keys [def-apply]} id agg events]
  (let [events (filter #(= (:id %) id) events)

        agg (event/create-aggregate agg events def-apply)]
    {:aggregate agg
     :version   (get agg :version 0)}))

(defn get-by-id-from-store
  [ctx id]
  (let [resp (try
               (event/get-by-id (assoc ctx :id id))
               (catch Exception e
                 (ex-data e)))]
    {:version   (get-in resp [:aggregate :version] 0)
     :aggregate (:aggregate resp)}))

(defn get-stored-aggregate! [ctx id]
  (if-let [agg-cached (get-in @request/*request* [:aggregate-db id])]
    agg-cached
    (let [agg-db (get-by-id-from-store ctx id)]
      (swap! request/*request* assoc-in [:aggregate-db id] agg-db)
      agg-db)))

(defn get-current-aggregate! [ctx id]
  (let [agg (get-stored-aggregate! ctx id)
        events (get-in @request/*request* [:events])

        curr-agg (apply-events ctx id (:aggregate agg) events)]
    (swap! request/*request* assoc-in [:last-event-seq id] (:version curr-agg))
    curr-agg))

(defn get-by-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             (:id query))]}
  (let [agg (get-current-aggregate! ctx (or (:id query) (:id ctx)))]

    (if query
      (or (:aggregate agg) nil)
      (merge ctx
             (if (:aggregate agg)
               agg
               {:aggregate nil})))))

(defn fetch-event-sequence-for-command
  [ctx cmd]
  (let [request (if (bound? #'request/*request*)
                  @request/*request*
                  {})
        id (:id cmd)
        last-seq (get-in request [:last-event-seq id])
        last-seq (or last-seq
                     (dal/get-max-event-seq
                      (assoc ctx :id id)))]
    (swap! request/*request*
           assoc-in
           [:last-event-seq id]
           last-seq)
    (assoc-in ctx [:last-event-seq id] last-seq)))

(defn flush-cache
  []
  (swap! request/*request* dissoc :events)
  (swap! request/*request* dissoc :aggregate-db)
  (swap! request/*request* dissoc :last-event-seq))

(defn clear! []
  (flush-cache))

(defn create-identity
  [& _]
  (uuid/gen))

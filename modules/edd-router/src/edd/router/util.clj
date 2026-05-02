(ns edd.router.util
  (:require [clojure.string :as string]
            [sdk.aws.sqs :as sqs]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [lambda.uuid :as uuid])
  (:import (java.security MessageDigest)
           (java.util.concurrent Executors
                                 ExecutorService
                                 Future)))

(defn sum256sha [string]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256") (.getBytes string "UTF-8"))]
    (apply str (map (partial format "%02x") digest))))

(defn check-deduplication-id
  "Because of limit on SQS"
  [s]
  (if (< (count s) 128)
    s
    (sum256sha s)))

(defn queue-name
  "Concatenates the queue-name out of the service-name, the stage [prod, test],
   and the queue-type [commands, events]."
  [ctx
   service
   {:keys [realm stage]
    :as _meta}
   type]
  (let [service
        (name service)

        stage
        (if stage
          stage
          (case realm
            :test "test"
            "prod"))

        type
        (case type
          :commands "commands-1.fifo"
          :events "events-1.fifo")]
    (string/join
     "-"
     [(:environment-name-lower ctx)
      stage service type])))

(def max-message-size 226214)
(def max-batch-count 10)

(defn partition-messages
  [messages]
  (loop [messages messages
         batch-size 0
         current-batch []
         batched []]
    (let [message (first messages)
          body (:body message)
          message-size (and body
                            (alength
                             (.getBytes ^String body)))]
      (cond
        (not message) (if (seq current-batch)
                        (conj batched current-batch)
                        batched)

        (= (count current-batch)
           max-batch-count) (recur messages
                                   0
                                   []
                                   (if (seq current-batch)
                                     (conj batched current-batch)
                                     batched))
        (and (> (+ batch-size
                   message-size)
                max-message-size)
             (< message-size max-message-size)) (recur (rest messages)
                                                       message-size
                                                       [message]
                                                       (if (seq current-batch)
                                                         (conj batched current-batch)
                                                         batched))
        (and (> (+ batch-size
                   message-size)
                max-message-size)
             (> message-size max-message-size)) (recur (rest messages)
                                                       0
                                                       []
                                                       (if (seq current-batch)
                                                         (conj batched current-batch message)
                                                         (conj batched message)))
        :else (recur (rest messages)
                     (+ batch-size message-size)
                     (conj current-batch message)
                     batched)))))

(defn map-effects
  [ctx effects start]
  (map-indexed
   (fn [index {:keys [service] :as cmd}]
     {:id               (str (+ index start))
      :queue            (queue-name ctx service (get-in cmd [:meta]) :commands)
      :body             (util/to-json cmd)
      :group-id         (or (get-in cmd [:meta :group-id])
                            (get-in cmd [:commands 0 :id]))
      :source           cmd
      :deduplication-id (str (uuid/gen))})
   effects))

(defn map-events
  [ctx service events start]
  (map-indexed
   (fn [index {:keys [id] :as event}]
     {:id               (str (+ index start))
      :queue            (queue-name ctx service (get-in event [:meta]) :events)
      :body             (util/to-json {:apply          {:aggregate-id id
                                                        :service      service}
                                       :meta           (:meta event)
                                       :request-id     (:request-id event)
                                       :interaction-id (:interaction-id event)})
      :source           event
      :group-id         id
      :deduplication-id (str (uuid/gen))})
   events))

(defn publish-batch
  [ctx batch]
  (let [queue (-> batch first :queue)
        id (-> batch first :id)]
    (try
      (sqs/sqs-publish-batch
       (assoc ctx
              :messages batch
              :queue queue))
      (catch Exception e
        (log/error (str "Message publish batch failed, queue: " queue
                        ", starting id: " id) e)
        (map (fn [{:keys [id]}]
               {:success false
                :id id})
             batch)))))

(defn publish-single
  [ctx {:keys [id queue] :as message}]
  (try
    (sqs/sqs-publish (assoc ctx
                            :queue queue
                            :message message))
    (catch Exception e
      (log/error (str "Message publish failed for queue: " queue ", id: " id) e)
      {:success false
       :id id})))

(def resonable-therads-count 100)

(defn do-execute
  [ctx batches]
  (let [nthreads (min (count batches)
                      resonable-therads-count)
        _ (log/info (str "Using threads: " nthreads
                         " to deliver: " (count batches)
                         " messages"))
        response (atom '())
        total (count batches)
        ^ExecutorService pool (Executors/newFixedThreadPool nthreads)
        tasks (map
               (fn [v]
                 #(let [resp (if (map? v)
                               (publish-single ctx v)
                               (publish-batch ctx v))
                        resp (swap! response conj resp)
                        processed (count resp)]
                    (when (= 0 (mod processed 100))
                      (log/info (str "Progress: " processed "/" total)))))
               batches)]
    (doseq [^Future future (.invokeAll pool tasks)]
      (.get future))
    (.shutdown pool)
    (log/info (str "Done: " (count @response) "/" (count batches)))
    @response))

(defn group-and-publish
  [ctx messages]
  (let [queue-groups (group-by :queue messages)
        batches (reduce
                 (fn [p group]
                   (into p (partition-messages group)))
                 []
                 (vals queue-groups))
        responses (when (> (count batches) 0)
                    (util/d-time
                     "Executing all deliveries"
                     (do-execute ctx batches)))]
    (doall
     (flatten responses))))

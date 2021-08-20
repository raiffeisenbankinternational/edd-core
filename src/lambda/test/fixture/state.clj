(ns lambda.test.fixture.state)

(def default-db
  {:global          true
   :event-store     []
   :identity-store  []
   :sequence-store  []
   :command-store   []
   :aggregate-store []
   :response-log    []
   :command-log     []})

(def ^:dynamic *dal-state* (atom default-db))

(def ^:dynamic *mock*)

(def ^:dynamic *queues* {:command-queue (atom [])
                         :seed 17})

(defmacro with-state
  [& body]
  `(binding [*mock* (atom {})]
     (do ~@body)))

(defn pop-item
  [key values]
  (let [state (swap! *mock* (fn [v]
                              (let [current (get v key)]
                                (if current
                                  v
                                  (assoc v key values)))))
        key-value (last (get state key))]
    (if key-value
      (do (swap! *mock* (fn [v]
                          (update v key pop)))
          key-value)
      nil)))

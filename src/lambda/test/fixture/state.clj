(ns lambda.test.fixture.state)

(def ^:dynamic *dal-state*)

(def ^:dynamic *mock*)

(def ^:dynamic *queues*)

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

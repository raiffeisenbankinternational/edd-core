(ns edd.test.fixture.dates
  (:require [lambda.util :as util]))

(def ^:dynamic *current-request-id* nil)

(def captured-dates (atom {}))

(def ^:private orig-date->string @#'util/date->string)

(defn -per-request-capture
  ([]
   (let [d
         (orig-date->string)]
     (when *current-request-id*
       (swap! captured-dates update *current-request-id* (fnil conj []) d))
     d))
  ([dt] (orig-date->string dt)))

(defn reset-captured-dates!
  []
  (reset! captured-dates {}))

(defn dates-for
  [request-id]
  (get @captured-dates request-id))

(defn per-request-fixture
  "Fixture for `use-fixtures :each`. Resets `captured-dates` and redefs
   `util/date->string` so emissions land in `captured-dates` keyed by the
   currently-bound `*current-request-id*`."
  []
  (fn [t]
    (reset-captured-dates!)
    (with-redefs [util/date->string -per-request-capture]
      (t))))

(defn with-captured-dates*
  "Functional form of `with-captured-dates`. Runs `thunk` with
   `util/date->string` redefed to record each emission, then returns the
   captured dates as a vector (in order)."
  [thunk]
  (let [captured
        (atom [])

        orig
        orig-date->string]

    (with-redefs [util/date->string
                  (fn
                    ([]
                     (let [d
                           (orig)]
                       (swap! captured conj d)
                       d))
                    ([dt] (orig dt)))]
      (thunk))
    @captured))

(defmacro with-captured-dates
  [& body]
  `(with-captured-dates* (fn [] ~@body)))

(ns edd.view-store.postgres.common
  "
  Common shortcuts and utilities.
  "
  (:require
   [clojure.string :as str]))

(defmacro error! [template & args]
  `(throw (new RuntimeException (format ~template ~@args))))

(defn ->realm
  [ctx]
  (or (some-> ctx :meta :realm name)
      (error! "realm is not set")))

(defn ->service
  [ctx]
  (or (:service-name ctx)
      (error! "service-name is not set")))

(defn ->long!
  "
  Try to coerce a thing to a long number
  throwing exceptions if something goes wrong.
  "
  [x]
  (cond
    (int? x)
    x
    (string? x)
    (try
      (Long/parseLong x)
      (catch Throwable _
        (error! "could not parse long: %s" x)))
    :else
    (error! "wrong long value: %s" x)))

(defn flatten-paths
  ([m separator]
   (flatten-paths m separator []))
  ([m separator path]
   (->> (map (fn [[k v]]
               (if (and (map? v) (not-empty v))
                 (flatten-paths v separator (conj path k))
                 [(->> (conj path k)
                       (map name)
                       (str/join separator)
                       keyword) v]))
             m)
        (into {}))))

(defn enumerate
  "
  Build a lazy seq of pairs like [i, <item>],
  where `i` is the current number (from zero).
  Can be initiated with index offset.
  "
  ([coll]
   (map-indexed vector coll))
  ([offset coll]
   (map-indexed (fn [i item]
                  [(+ offset i) item])
                coll)))

(defn vbutlast
  "
  Like butlast but for vector. As subvec is O(1),
  we won't traverse the whole vector.
  "
  [vect]
  (if (= 0 (count vect))
    []
    (subvec vect 0 (dec (count vect)))))

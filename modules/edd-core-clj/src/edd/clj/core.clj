(ns edd.clj.core
  "
  This namespace holds functions that mimic the same stuff
  from Clojure 1.11. We still cannot use Clojure 1.11
  due to our native-image compilation issues. Some of the
  functions slightly differ from their origin implementations,
  e.g. accept extra arguments.
  "
  (:refer-clojure :exclude [update-vals
                            update-keys
                            parse-long
                            random-uuid
                            parsing-err
                            abs])
  (:import
   clojure.lang.IEditableCollection
   java.util.UUID))

(defn- parsing-err ^String [val]
  (str "Expected string, got " (if (nil? val) "nil" (-> val class .getName))))

(defn parse-long
  ^Long [^String s]
  (if (string? s)
    (try
      (Long/valueOf s)
      (catch NumberFormatException _ nil))
    (throw (IllegalArgumentException. (parsing-err s)))))

(defn abs ^Number [^Number n]
  (if (neg? n) (- n) n))

(defn random-uuid ^UUID []
  (UUID/randomUUID))

(defn update-keys
  "
  Like update-keys from Clojure 11, but accepts extra arguments.
  "
  [m f & args]
  (let [ret (persistent!
             (reduce-kv (fn [acc k v] (assoc! acc (apply f k args) v))
                        (transient {})
                        m))]
    (with-meta ret (meta m))))

(defn update-vals
  "
  Like update-vals from Clojure 11, but accepts extra arguments.
  "
  [m f & args]
  (with-meta
    (persistent!
     (reduce-kv (fn [acc k v] (assoc! acc k (apply f v args)))
                (if (instance? IEditableCollection m)
                  (transient m)
                  (transient {}))
                m))
    (meta m)))

(defn each
  "
  A helper function to use with `update(-in)` for nested
  sequential values, for example:

  (update-in data [:path :to :numbers] each + 100)
  (update-in data [:path :to :users] each update-user some-mapping)

  "
  [items f & args]
  (persistent!
   (reduce
    (fn [acc! item]
      (conj! acc! (apply f item args)))
    (-> items empty transient)
    items)))

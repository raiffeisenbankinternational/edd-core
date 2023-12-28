(ns lambda.uuid
  (:import (java.util UUID)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn gen
  []
  (UUID/randomUUID))

(defn parse
  [id]
  (when id
    (if (= (type id) UUID)
      id
      (UUID/fromString id))))

(defn named
  [^String name]
  (UUID/nameUUIDFromBytes (.getBytes name)))

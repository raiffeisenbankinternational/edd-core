(ns lambda.uuid
  (:import (java.util UUID)))


(defn gen
  []
  (UUID/randomUUID))

(defn parse
  [id]
  (when id
    (if (= (type id) UUID)
      id
      (UUID/fromString id))))

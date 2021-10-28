(ns plc2.buckets
  (:require
   [malli.core :as m]))

(defn assoc-some [m k v]
  (if (some? v)
    (assoc m k v)
    m))

(def valid-buckets
  (reduce into
          []
          [(for [d (range 0 7)]
             {:days d})

           (rest (for [y [nil 1 2]
                       m [nil 1 2 3 4 5 6 7 8 9 10 11]
                       w [nil 1 2 3]]
                   (-> {}
                       (assoc-some :years y)
                       (assoc-some :months m)
                       (assoc-some :weeks w))))

           (for [y [3 4 5 6]
                 m [nil 1 2 3 4 5 6 7 8 9 10 11]]
             (-> {}
                 (assoc-some :years y)
                 (assoc-some :months m)))

           (for [y [7 8 9]
                 m [nil 6]]
             (-> {}
                 (assoc-some :years y)
                 (assoc-some :months m)))

           (for [y (range 10 101)]
             (-> {}
                 (assoc-some :years y)))]))

(def BucketDefinition
  (m/schema
   [:and
    (into [:enum {}] valid-buckets)
    [:map {:min 1}
     [:years {:optional true} [:and pos-int? [:<= 100]]]
     [:months {:optional true} [:and pos-int? [:<= 11]]]
     [:days {:optional true} [:and int? [:<= 6] [:>= 0]]]
     [:weeks {:optional true} [:and pos-int? [:<= 3]]]]]))

(ns edd.search)

(defn parse [ctx q]
  (let [% (first q)]
    (cond
      (vector? %) (recur ctx %)
      :else (apply (get ctx %) ctx (rest q)))))

(defmulti update-aggregate
          (fn [ctx] (:view-store ctx)))

(defmulti advanced-search
          (fn [ctx] (:view-store ctx)))

(defmulti simple-search
          (fn [{:keys [query] :as ctx}] (:view-store ctx)))

(defmulti update-aggregate
          (fn [{:keys [aggregate] :as ctx}] (:view-store ctx)))

(def default-size 50)

(ns edd.elastic.view-store
  (:gen-class)
  (:require [clojure.test :refer :all]
            [lambda.elastic :as el]
            [edd.search :refer [with-init
                                simple-search
                                default-size
                                advanced-search
                                update-aggregate]]
            [clojure.pprint :refer [pprint]]
            [lambda.util :as util]
            [lambda.elastic :as elastic]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [edd.search :refer [parse]]))


(defn ->trim [v]
  (if (string? v)
    (str/trim v)
    v))

(def el
  {:and      (fn [ctx & rest]
               {:bool
                {:filter (mapv
                           (fn [%] (parse ctx %))
                           rest)}})
   :or       (fn [ctx & rest]
               {:bool
                {:should               (mapv
                                         (fn [%] (parse ctx %))
                                         rest)
                 :minimum_should_match 1}})
   :eq       (fn [_ & [a b]]
               {:term
                {(str (name a) ".keyword") (->trim b)}})
   :wildcard (fn [_ & [a b]]
               {:wildcard
                {(str (name a) ".keyword") {:value (str "*" (->trim b) "*")}}})
   :not      (fn [ctx & rest]
               {:bool
                {:must_not (parse ctx rest)}})
   :in       (fn [_ & [a b]]
               {:terms
                {(str (name a) ".keyword") b}})
   :exists   (fn [_ & [a _]]
               {:exists
                {:field (name a)}})})

(defn search-filter
  [_ filter q]
  (let [[fields-key fields value-key value] (:search q)
        search (mapv
                 (fn [p]
                   {:wildcard
                    {(str (name p) ".keyword") {:value (str "*" (->trim value) "*")}}})
                 fields)]

    (-> filter
        (assoc-in [:query :bool :should] search)
        (assoc-in [:query :bool :minimum_should_match] 1))))


(defn form-sorting
  [sort]
  (map
    (fn [[a b]]
      (case (keyword b)
        :asc {(str (name a) ".keyword") {:order "asc"}}
        :desc {(str (name a) ".keyword") {:order "desc"}}
        :asc-number {(str (name a) ".number") {:order "asc"}}
        :desc-number {(str (name a) ".number") {:order "desc"}}
        :asc-date {(name a) {:order "asc"}}
        :desc-date {(name a) {:order "desc"}}))
    (partition 2 sort)))

(defn create-query
  [ctx q]
  (let [filter (if (:filter q)
                 {:query {:bool
                          {:filter (parse el (:filter q))}}}
                 {})
        query (if (:search q)
                (search-filter ctx filter q)
                filter)

        select-query (if (:select q)
                       (assoc query
                         :_source
                         (mapv
                           name
                           (:select q)))
                       query)
        sort-query (if (:sort q)
                     (assoc select-query
                       :sort
                       (form-sorting (:sort q)))

                     select-query)]
    sort-query))

(defmethod advanced-search
  :elastic
  [{:keys [query] :as ctx}]
  (let [req (create-query ctx query)
        body (el/query
               (assoc ctx
                 :method "POST"
                 :path (str "/" (str/replace (get ctx :index-name
                                                  (:service-name ctx)) "-" "_") "/_search")
                 :body (util/to-json (assoc req
                                       :from (get query :from 0)
                                       :size (get query :size default-size)))))
        total (get-in
                body
                [:hits :total :value])]
    (log/info "Elastic query")
    (log/info (util/to-json query))
    (log/info body)
    {:total total
     :from  (get query :from 0)
     :size  (get query :size default-size)
     :hits  (mapv
              (fn [%]
                (get % :_source))
              (get-in
                body
                [:hits :hits]
                []))}))

(defn flatten-paths
  ([m separator]
   (flatten-paths m separator []))
  ([m separator path]
   (->> (map (fn [[k v]]
               (if (and (map? v) (not-empty v))
                 (flatten-paths v separator (conj path k))
                 [(->> (conj path k)
                       (map name)
                       (clojure.string/join separator)
                       keyword) v]))
             m)
        (into {}))))

(defn- add-to-keyword [kw app-str]
  (keyword (str (name kw)
                app-str)))

(defn create-simple-query
  [query]
  {:pre [query]}
  (println (flatten-paths query "."))
  (util/to-json
    {:size  600
     :query {:bool
             {:must (mapv
                      (fn [%]
                        {:term {(add-to-keyword (first %) ".keyword")
                                (second %)}})
                      (seq (flatten-paths query ".")))}}}))

(defmethod simple-search
  :elastic
  [{:keys [query] :as ctx}]
  (log/debug "Executing simple search" query)
  (let [index (str/replace (get ctx :index-name
                                (:service-name ctx)) "-" "_")
        param (dissoc query :query-id)
        body (elastic/query
               (assoc ctx
                 :method "POST"
                 :path (str "/" index "/_search")
                 :body (create-simple-query param)))]
    (mapv
      (fn [%]
        (get % :_source))
      (get-in
        body
        [:hits :hits]
        []))))

(defmethod update-aggregate
  :elastic
  [{:keys [aggregate] :as ctx}]
  (log/debug "Updating aggregate" aggregate)
  (let [index (str/replace (:service-name ctx) "-" "_")]
    (elastic/query (assoc ctx
                     :method "POST"
                     :path (str "/" index "/_doc/" (:id aggregate))
                     :body (util/to-json aggregate)))))

(defmethod with-init
  :elastic
  [ctx body-fn]
  (log/debug "Initializing")
  (body-fn ctx))

(defn register
  [ctx]
  (assoc ctx :view-store :elastic))
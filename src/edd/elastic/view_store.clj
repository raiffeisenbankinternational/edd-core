(ns edd.elastic.view-store
  (:gen-class)
  (:require [clojure.test :refer :all]
            [lambda.elastic :as el]
            [edd.search :refer [with-init
                                simple-search
                                default-size
                                advanced-search
                                update-aggregate
                                get-snapshot]]
            [lambda.util :as util]
            [lambda.elastic :as elastic]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [edd.search :refer [parse]]))

(defn ->trim [v]
  (if (string? v)
    (str/trim v)
    v))

(defn realm
  [ctx]
  (name (get-in ctx [:meta :realm] :no_realm)))

(def el
  {:and (fn [ctx & rest]
          {:bool
           {:filter (mapv
                     (fn [%] (parse ctx %))
                     rest)}})
   :or (fn [ctx & rest]
         {:bool
          {:should (mapv
                    (fn [%] (parse ctx %))
                    rest)
           :minimum_should_match 1}})
   :eq (fn [_ & [a b]]
         {:term
          {(str (name a) ".keyword") (->trim b)}})
   := (fn [_ & [a b]]
        {:term
         {(name a) (->trim b)}})
   :wildcard (fn [_ & [a b]]
               {:wildcard
                {(str (name a)) {:value (str "*" (->trim b) "*")}}})
   :not (fn [ctx & rest]
          {:bool
           {:must_not (parse ctx rest)}})
   :in (fn [_ & [a b]]
         {:terms
          {(str (name a) ".keyword") b}})
   :exists (fn [_ & [a _]]
             {:exists
              {:field (name a)}})
   :lte (fn [_ & [a b]]
          {:range
           {(name a) {:lte b}}})
   :gte (fn [_ & [a b]]
          {:range
           {(name a) {:gte b}}})
   :nested (fn [ctx path & rest]
             {:bool
              {:must [{:nested {:path (name path)
                                :query (mapv
                                        (fn [%] (parse ctx %))
                                        rest)}}]}})})

(defn search-filter
  [_ filter q]
  (let [[fields-key fields value-key value] (:search q)
        search (mapv
                (fn [p]
                  {:bool
                   {:should [{:match
                              {(str (name p))
                               {:query value
                                :boost 2}}}
                             {:wildcard
                              {(str (name p)) {:value (str "*" (->trim value) "*")}}}]
                    :minimum_should_match 1}})
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

(defn advanced-direct-search
  [ctx es-qry]
  (let [json-qry (util/to-json es-qry)
        {:keys [error] :as body} (el/query
                                  (assoc ctx
                                         :method "POST"
                                         :path (str "/"
                                                    (realm ctx)
                                                    "_"
                                                    (str/replace (get ctx :index-name
                                                                      (name (:service-name ctx))) "-" "_") "/_search")
                                         :body json-qry))
        total (get-in
               body
               [:hits :total :value])]

    (when error
      (throw (ex-info "Elastic query failed" error)))
    (log/debug "Elastic query")
    (log/debug json-qry)
    (log/debug body)
    {:total total
     :from (get es-qry :from 0)
     :size (get es-qry :size default-size)
     :hits (mapv
            (fn [%]
              (get % :_source))
            (get-in
             body
             [:hits :hits]
             []))}))

(defmethod advanced-search
  :elastic
  [{:keys [query] :as ctx}]
  (let [req (->
             (create-query ctx query)
             (assoc
              :from (get query :from 0)
              :size (get query :size default-size)))]
    (advanced-direct-search ctx req)))

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
  (util/to-json
   {:size 600
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
                                (name (:service-name ctx))) "-" "_")
        param (dissoc query :query-id)
        body (elastic/query
              (assoc ctx
                     :method "POST"
                     :path (str "/"
                                (realm ctx)
                                "_"
                                index "/_search")
                     :body (create-simple-query param)))]
    (mapv
     (fn [%]
       (get % :_source))
     (get-in
      body
      [:hits :hits]
      []))))

(defn store-to-elastic
  [{:keys [aggregate] :as ctx}]
  (log/debug "Updating aggregate" aggregate)
  (let [index (-> ctx
                  (:service-name)
                  (name)
                  (str/replace "-" "_"))
        {:keys [error]} (elastic/query (assoc ctx
                                              :method "POST"
                                              :path (str "/"
                                                         (realm ctx)
                                                         "_"
                                                         index "/_doc/" (:id aggregate))
                                              :body (util/to-json aggregate)))]
    (if error
      (throw (ex-info "could not store aggregate" {:error error}))
      ctx)))

(defmethod update-aggregate
  :elastic
  [{:keys [aggregate] :as ctx}]
  (store-to-elastic ctx))

(defmethod with-init
  :elastic
  [ctx body-fn]
  (log/debug "Initializing")
  (body-fn ctx))

(defn register
  [ctx]
  (assoc ctx
         :view-store :elastic
         :elastic-search {:scheme (util/get-env "IndexDomainScheme" "https")
                          :url (util/get-env "IndexDomainEndpoint")}))

(defmethod get-snapshot
  :elastic
  [ctx id]
  (log/info "Fetching snapshot aggregate" id)
  (let [index (-> ctx
                  (:service-name)
                  (name)
                  (str/replace "-" "_"))
        {:keys [error body]} (elastic/query (assoc (dissoc ctx :body)
                                                   :method "GET"
                                                   :path (str "/"
                                                              (realm ctx)
                                                              "_"
                                                              index "/_doc/" id)))]
    (if error
      nil
      body)))
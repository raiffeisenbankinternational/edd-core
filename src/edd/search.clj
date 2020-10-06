(ns edd.search
  (:gen-class)
  (:require [clojure.test :refer :all]
            [lambda.elastic :as el]
            [clojure.pprint :refer [pprint]]
            [lambda.util :as util]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn parse [ctx q]
  (let [% (first q)]
    (cond
      (vector? %) (recur ctx %)
      :else (apply (get ctx %) ctx (rest q)))))

(defn ->trim [v]
  (if (string? v)
    (str/trim v)
    v))

(def el
  {:and (fn [ctx & rest]
          {:bool
           {:filter (mapv
                     (fn [%] (parse ctx %))
                     rest)}})
   :or  (fn [ctx & rest]
          {:bool
           {:should (mapv
                     (fn [%] (parse ctx %))
                     rest)
            :minimum_should_match 1}})
   :eq  (fn [_ & [a b]]
          {:term
           {(str (name a) ".keyword") (->trim b)}})
   :wildcard (fn [_ & [a b]]
               {:wildcard
                {(str (name a) ".keyword") {:value (str "*" (->trim b) "*")}}})
   :not (fn [ctx & rest]
          {:bool
           {:must_not (parse ctx rest)}})
   :in  (fn [_ & [a b]]
          {:terms
           {(str (name a) ".keyword") b}})
   :exists (fn [_ & [a _]]
             {:exists
              {:field (name a)}})})

(defn search-filter
  [ctx filter q]
  (let [[fields-key fields value-key value] (:search q)
        search (mapv
                (fn [p]
                  {:wildcard
                   {(str (name p) ".keyword") {:value (str "*" (->trim value) "*")}}})
                fields)]

    (-> filter
        (assoc-in [:query :bool :should] search)
        (assoc-in [:query :bool :minimum_should_match] 1))))

(def default-size 50)

(defn form-sorting
  [sort]
  (map
   (fn [[a b]]
     (case (keyword b)
       :asc {(str (name a) ".keyword") {:order "asc"}}
       :desc {(str (name a) ".keyword") {:order "desc"}}
       :asc-number {(str (name a) ".number") {:order "asc"}}
       :desc-number {(str (name a) ".number") {:order "desc"}}))
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

(defn advanced-search
  [ctx q]
  (let [query (create-query ctx q)
        body (el/query
              "POST"
              (str "/" (str/replace (get ctx :index-name
                                         (:service-name ctx)) "-" "_") "/_search")
              (util/to-json (assoc query
                                   :from (get q :from 0)
                                   :size (get q :size default-size))))
        total (get-in
               body
               [:hits :total :value])]
    (log/info "Elastic query")
    (log/info (util/to-json query))
    (log/info body)
    {:total total
     :from  (get q :from 0)
     :size  (get q :size default-size)
     :hits  (mapv
             (fn [%]
               (get % :_source))
             (get-in
              body
              [:hits :hits]
              []))}))

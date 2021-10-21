(ns edd.schema.repl
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.schema.core :as schema-core]
            [edd.schema.swagger :as swagger]))


(deftest local
  (swagger/swagger-runtime
    (-> {:edd/schema-out    "/home/wzhpor/Downloads/swagger-ui/swagger.yaml"
         :edd/schema-format "yaml"}
        (edd/reg-cmd :dummy-cmd (fn [ctx cmd]
                                  [])
                     :spec [:map
                            [:name string?]])
        (edd/reg-cmd :dummy-cmd-1 (fn [ctx cmd]
                                  [])
                     :spec [:map
                            [:name string?]])
        (edd/reg-query :query-1 (fn [ctx query]
                                  [])
                       :produces [:map
                                  [:result
                                   [:map
                                    [:first-name :string]
                                    [:last-name :string]]]]
                       :consumes [:map
                                  [:user
                                   [:map
                                    [:email :string]]]]))))


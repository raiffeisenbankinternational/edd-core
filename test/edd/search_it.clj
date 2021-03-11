(ns edd.search-it
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [edd.memory.view-store :as memory-view-store]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.search :as search]
            [lambda.test.fixture.state :as state]
            [edd.search :as search]
            [lambda.elastic :as el]
            [lambda.uuid :as uuid]
            [clojure.string :as str]))

(def ctx
  {:elastic-search {:url (util/get-env "IndexDomainEndpoint")}
   :aws            {:region                (util/get-env "AWS_DEFAULT_REGION")
                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}})
(defn load-data
  [ctx]
  (doseq [i (:aggregate-store @state/*dal-state*)]
    (log/info (el/query
               (assoc ctx
                      :method "POST"
                      :path (str "/" (:service-name ctx) "/_doc")
                      :body (util/to-json
                             i))))))

(defn test-query
  [data q]
  (binding [state/*dal-state* (atom {:aggregate-store data})]
    (let [service-name (str/replace (str "test-" (uuid/gen)) "-" "_")
          local-ctx (assoc ctx :service-name service-name)
          body {:settings
                {:index
                 {:number_of_shards   1
                  :number_of_replicas 0}}
                :mappings
                {:dynamic_templates
                 [{:integers
                   {:match_mapping_type "long",
                    :mapping
                    {:type "integer",
                     :fields
                     {:number {:type "long"},
                      :keyword
                      {:type         "keyword",
                       :ignore_above 256}}}}}]}}]

      (log/info "Index name" service-name)
      (el/query
       (assoc local-ctx
              :method "PUT"
              :path (str "/" service-name)
              :body (util/to-json body)))
      (load-data local-ctx)
      (Thread/sleep 2000)
      (let [el-result (search/advanced-search (-> local-ctx
                                                  (elastic-view-store/register)
                                                  (assoc :query q)))
            mock-result (search/advanced-search (-> local-ctx
                                                    (memory-view-store/register)
                                                    (assoc :query q)))]
        (log/info el-result)
        (log/info mock-result)
        (el/query
         (assoc local-ctx
                :method "DELETE"
                :path (str "/" service-name)))
        [el-result mock-result]))))

(deftest test-elastic-mock-parity-1
  (let [data [{:k1 "121" :k2 "be" :k3 {:k31 "c"}}
              {:k1 "758" :k2 "be"}
              {:k1 "121" :k2 "be" :k3 {:k31 "d"}}]
        query {:filter [:and
                        [:and
                         [:eq :k1 "121"]
                         [:eq :k2 "be"]]
                        [:eq "k3.k31" "c"]]}
        expected {:total 1
                  :from  0
                  :size  50
                  :hits  [{:k1 "121" :k2 "be" :k3 {:k31 "c"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-2
  (let [data [{:k1 "121" :k2 "be" :k3 {:k31 "c"}}
              {:k1 "758" :k2 "be"}
              {:k1 "751" :k2 "be"}
              {:k1 "751" :k2 "bb"}
              {:k1 "121" :k2 "be" :k3 {:k31 "d"}}]
        query {:filter [:and
                        [:eq :k2 "be"]
                        [:in :k1 ["751" "758"]]]}
        expected {:total 2
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 "be"}
                          {:k1 "751" :k2 "be"}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-keyword-in-2
  (let [data [{:k1 :v0 :k2 "be" :k3 {:k31 "c"}}
              {:k1 :v1 :k2 "bi"}
              {:k1 :v1 :k2 "be"}
              {:k1 :v2 :k2 "be"}
              {:k1 :v1 :k2 "bb"}
              {:k1 :v3 :k2 "be" :k3 {:k31 "d"}}]
        query {:filter [:and
                        [:eq :k2 "be"]
                        [:in :k1 [:v1 :v2]]]}
        expected {:total 2
                  :from  0
                  :size  50
                  :hits  [{:k1 :v1 :k2 "be"}
                          {:k1 :v2 :k2 "be"}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-3
  (let [data [{:k1 "121" :k2 "be" :k3 {:k31 "c"}}
              {:k1 "758" :k2 "be"}
              {:k1 "751" :k2 "be"}
              {:k1 "751" :k2 "bb"}
              {:k1 "121" :k2 "be" :k3 {:k31 "d"}}]
        query {:filter [:and
                        [:eq :k2 "be"]
                        [:not
                         [:in :k1 ["751" "758"]]]]}
        expected {:total 2
                  :from  0
                  :size  50
                  :hits  [{:k1 "121"
                           :k2 "be"
                           :k3 {:k31 "c"}}
                          {:k1 "121"
                           :k2 "be"
                           :k3 {:k31 "d"}}]}

        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-4
  (let [data [{:k1 "121" :attrs {:type :booking-company}}
              {:k1 "122" :attrs {:type :breaking-company}}
              {:k1 "123" :attrs {:type :booking-company}}]
        query {:filter [:not
                        [:eq "attrs.type" :booking-company]]}
        expected
        {:total 1
         :from  0
         :size  50
         :hits  [{:k1 "122" :attrs {:type :breaking-company}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-search-1
  (let [data [{:k1 "consectetur" :attrs {:type :booking-company}}
              {:k1 "lorem" :k2 "1adc" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "2abc" :attrs {:type :breaking-company}}
              {:k1 "dolor" :k2 "3abc" :attrs {:type :breaking-company}}
              {:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "5abc" :attrs {:type :breaking-company}}
              {:k1 "dol" :k2 "6abc" :attrs {:type :breaking-company}}
              {:k1 "amet" :k2 "7abc" :attrs {:type :booking-company}}]
        query {:search [:fields [:k1 :k2]
                        :value "or"]
               :filter [:not
                        [:eq "attrs.type" :booking-company]]}
        expected {:total 3
                  :from  0
                  :size  50
                  :hits  [{:k1 "lorem" :k2 "1adc" :attrs {:type :breaking-company}}
                          {:k1 "dolor" :k2 "3abc" :attrs {:type :breaking-company}}
                          {:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}]}

        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-search-only-1
  (let [data [{:k1 "consectetur" :attrs {:type :booking-company}}
              {:k1 "lorem" :k2 "1adc" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "2abc" :attrs {:type :breaking-company}}
              {:k1 "dolor" :k2 "3abc" :attrs {:type :breaking-company}}
              {:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "5abc" :attrs {:type :breaking-company}}
              {:k1 "dol" :k2 "6abc" :attrs {:type :breaking-company}}
              {:k1 "amet" :k2 "7aor" :attrs {:type :booking-company}}]
        query {:search [:fields [:k1 :k2]
                        :value "or"]}
        expected {:total 4
                  :from  0
                  :size  50
                  :hits  [{:k1 "lorem" :k2 "1adc" :attrs {:type :breaking-company}}
                          {:k1 "dolor" :k2 "3abc" :attrs {:type :breaking-company}}
                          {:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}
                          {:k1 "amet" :k2 "7aor" :attrs {:type :booking-company}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-search-only-2
  (let [data [{:k1 "consectetur" :attrs {:type :booking-company}}
              {:k1 "lorem" :k2 "1adc" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "2abc" :attrs {:type :breaking-company}}
              {:k1 "dolor" :k2 "3abc" :attrs {:type :breaking-company}}
              {:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}
              {:k1 "ipsum" :k2 "5abc" :attrs {:type :breaking-company}}
              {:k1 "dol" :k2 "6abc" :attrs {:type :breaking-company}}
              {:k1 "amet" :k2 "7aor" :attrs {:type :booking-company}}
              {:k1 "ame5" :k2 "7aor" :attrs {:type :booking-company}}]
        query {:search [:fields [:k1 :k2]
                        :value "or"]
               :from   2
               :size   2}
        expected {:total 5
                  :from  2
                  :size  2
                  :hits  [{:k1 "lem" :k2 "4adcor" :attrs {:type :breaking-company}}
                          {:k1 "amet" :k2 "7aor" :attrs {:type :booking-company}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-search-only-3
  (let [data [{:attrs {:top-parent-id #uuid "1111a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "3333a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "4444a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "5555a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "8888a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}]
        query {:filter [:or
                        [:eq :attrs.top-parent-id #uuid "1111a4b3-1c40-48ec-b6c9-b9cceeea6bb8"]
                        [:eq :attrs.top-gcc-id #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"]]}
        expected {:total 2
                  :from  0
                  :size  50
                  :hits  [{:attrs {:top-parent-id #uuid "1111a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                                   :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
                          {:attrs {:top-parent-id #uuid "5555a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                                   :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-select-filter-2
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "d" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "e" :k22 "3"}}]
        query {:select ["k1" "k2.k21"]
               :size   50
               :filter [:eq "k2.k22" "2"]}
        expected {:total 3
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 {:k21 "c"}}
                          {:k1 "751" :k2 {:k21 "c"}}
                          {:k1 "751" :k2 {:k21 "d"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-select-filter-keywords
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "d" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "e" :k22 "3"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]}
        expected {:total 3
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 {:k21 "c"}}
                          {:k1 "751" :k2 {:k21 "c"}}
                          {:k1 "751" :k2 {:k21 "d"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-asc
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "752" :k2 {:k21 "d" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "e" :k22 "3"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]
               :sort   [:k1 "asc"]}
        expected {:total 3
                  :from  0
                  :size  50
                  :hits  [{:k1 "751" :k2 {:k21 "c"}}
                          {:k1 "752" :k2 {:k21 "d"}}
                          {:k1 "758" :k2 {:k21 "c"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-desc
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "752" :k2 {:k21 "d" :k22 "2"}}
              {:k1 "751" :k2 {:k21 "c" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "e" :k22 "3"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]
               :sort   [:k1 "desc"]}
        expected {:total 3
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 {:k21 "c"}}
                          {:k1 "752" :k2 {:k21 "d"}}
                          {:k1 "751" :k2 {:k21 "c"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-desc-1
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "1" :k22 "2"}}
              {:k1 "758" :k2 {:k21 "2" :k22 "2"}}
              {:k1 "250" :k2 {:k21 "3" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "4" :k22 "2"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]
               :sort   [:k1 "desc"
                        :k2.k21 "asc"]}
        expected {:total 4
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 {:k21 "1"}}
                          {:k1 "758" :k2 {:k21 "2"}}
                          {:k1 "250" :k2 {:k21 "3"}}
                          {:k1 "121" :k2 {:k21 "4"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-desc-2
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "1" :k22 "2"}}
              {:k1 "758" :k2 {:k21 "2" :k22 "2"}}
              {:k1 "250" :k2 {:k21 "3" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "4" :k22 "2"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]
               :sort   [:k1 "desc"
                        :k2.k21 "desc"]}
        expected {:total 4
                  :from  0
                  :size  50
                  :hits  [{:k1 "758" :k2 {:k21 "2"}}
                          {:k1 "758" :k2 {:k21 "1"}}
                          {:k1 "250" :k2 {:k21 "3"}}
                          {:k1 "121" :k2 {:k21 "4"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-asc-1
  (let [data [{:k1 "121" :k2 {:k21 "a" :k22 "1"}}
              {:k1 "758" :k2 {:k21 "1" :k22 "2"}}
              {:k1 "758" :k2 {:k21 "2" :k22 "2"}}
              {:k1 "250" :k2 {:k21 "3" :k22 "2"}}
              {:k1 "121" :k2 {:k21 "4" :k22 "2"}}]
        query {:select [:k1 :k2.k21]
               :size   50
               :filter [:eq "k2.k22" "2"]
               :sort   [:k1 "asc"
                        :k2.k21 "desc"]}
        expected {:total 4
                  :from  0
                  :size  50
                  :hits  [{:k1 "121" :k2 {:k21 "4"}}
                          {:k1 "250" :k2 {:k21 "3"}}
                          {:k1 "758" :k2 {:k21 "2"}}
                          {:k1 "758" :k2 {:k21 "1"}}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-asc-2
  (let [data [{:k1 1}
              {:k1 20}
              {:k1 3}
              {:k1 400}
              {:k1 5}]
        query {:size 50
               :sort [:k1 "asc-number"]}
        expected {:total 5
                  :from  0
                  :size  50
                  :hits  [{:k1 1}
                          {:k1 3}
                          {:k1 5}
                          {:k1 20}
                          {:k1 400}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-sort-desc-2
  (let [data [{:k1 1}
              {:k1 20}
              {:k1 3}
              {:k1 400}
              {:k1 5}]
        query {:size 50
               :sort [:k1 :desc-number]}
        expected {:total 5
                  :from  0
                  :size  50
                  :hits  [{:k1 400}
                          {:k1 20}
                          {:k1 5}
                          {:k1 3}
                          {:k1 1}]}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-exists-field-1
  (let [data [{:attrs {:top-parent-id #uuid "1111a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "3333a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "4444a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "5555a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "8888a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}]
        query {:filter [:exists :attrs.top-parent-id]}
        expected {:total 4
                  :from  0
                  :size  50
                  :hits  data}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))

(deftest test-elastic-mock-parity-test-exists-field-2
  (let [data [{:attrs {:top-parent-id #uuid "1111a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "3333a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "4444a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "5555a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}
              {:attrs {:top-parent-id #uuid "2222a4b3-1c40-48ec-b6c9-b9cceeea6bb8"
                       :top-gcc-id    #uuid "8888a4b3-1c40-48ec-b6c9-b9cceeea6bb8"}}]
        query {:filter [:not [:exists :attrs.top-parent-id]]}
        expected {:total 0
                  :from  0
                  :size  50
                  :hits  []}
        [el-result mock-result] (test-query data query)]
    (is (= expected
           el-result))
    (is (= expected
           mock-result))))


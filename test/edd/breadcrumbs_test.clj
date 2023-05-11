(ns edd.breadcrumbs-test
  (:require
   [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]
   [edd.core :as edd]
   [edd.memory.event-store :as event-store]
   [edd.test.fixture.execution :as exec]
   [edd.common :as common]
   [lambda.test.fixture.state :as state]
   [lambda.uuid :as uuid]
   [edd.test.fixture.dal :as mock]
   [clojure.string :as str]))

(def ctx
  (-> mock/ctx
      (assoc :service-name :local-test)
      (edd/reg-cmd :inc
                   (fn [ctx cmd]
                     {:event-id :inced
                      :level    (or (:level cmd) 0)})
                   :deps {:counter
                          (fn [_ctx cmd]
                            {:query-id :get-by-id
                             :id       (:id cmd)})})

      (edd/reg-fx (fn [_ctx evts]
                    (for [evt evts]
                      (let [level (inc (:level evt))]
                        (when (< level 3)
                          [{:commands [{:cmd-id :inc
                                        :level  level
                                        :id     (:id evt)}
                                       {:cmd-id :inc
                                        :level  level
                                        :id     (:id evt)}]}
                           {:commands [{:cmd-id :inc
                                        :level  level
                                        :id     (:id evt)}
                                       {:cmd-id :inc
                                        :level  level
                                        :id     (:id evt)}]}])))))

      (edd/reg-query :get-by-id common/get-by-id)
      (edd/reg-event :inced (fn [ctx event]
                              {:level (:level event)}))))

(def id1 (uuid/parse "111111-1111-1111-1111-111111111111"))

(defn sort-crumbs
  [resp]
  (sort-by #(str/join "-" (:breadcrumbs %)) resp))

(def command-store-with-bc
  (sort-crumbs
   [{:service :local-test
     :breadcrumbs [0
                   0]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  1}
                   {:cmd-id :inc
                    :id     id1
                    :level  1}]}
    {:service :local-test
     :breadcrumbs [0
                   1]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  1}
                   {:cmd-id :inc
                    :id     id1
                    :level  1}]}
    {:service :local-test
     :breadcrumbs [0
                   0
                   0]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   0
                   1]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   0
                   2]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   0
                   3]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   1
                   0]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   1
                   1]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   1
                   2]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}
    {:service :local-test
     :breadcrumbs [0
                   1
                   3]
     :commands    [{:cmd-id :inc
                    :id     id1
                    :level  2}
                   {:cmd-id :inc
                    :id     id1
                    :level  2}]}]))

(deftest test-breadcrumbs-for-emtpy-command
  (mock/with-mock-dal
    (with-redefs
     [event-store/clean-commands (fn [cmd] (dissoc cmd
                                                   :request-id
                                                   :interaction-id
                                                   :meta))]

      (exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                      :id     id1}]})
      (let [cmds (set (:command-store @state/*dal-state*))]
        (is (= command-store-with-bc
               (sort-crumbs cmds)))))))

(deftest test-breadcrumbs-for-command-with-breadcrumb
  (mock/with-mock-dal
    (with-redefs
     [event-store/clean-commands (fn [cmd]
                                   (dissoc cmd
                                           :request-id
                                           :interaction-id
                                           :meta))]

      (exec/run-cmd! ctx {:commands    [{:cmd-id :inc
                                         :id     id1}]
                          :breadcrumbs [0]})
      (let [cmds (:command-store @state/*dal-state*)]
        (is (= command-store-with-bc
               (sort-crumbs cmds)))))))

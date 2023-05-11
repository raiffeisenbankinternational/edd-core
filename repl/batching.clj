(ns batching
  (:require
   [edd.core :as edd]
   [edd.common :as common]
   [lambda.uuid :as uuid]
   [edd.test.fixture.dal :as mock]
   [edd.dal :as dal]))

(require '[edd.test.fixture.execution :as exec])
(require '[lambda.test.fixture.state
           :as state])
(require '[lambda.request :as r])

(def pp clojure.pprint/pprint)

(defn clear! []
  (reset! state/*dal-state* state/default-db)
  (reset! r/*request* {}))

(def ctx
  (-> mock/ctx
      (assoc :service-name :local-test)
      (edd/reg-cmd :inc
                   (fn [ctx cmd]
                     {:event-id :inced
                      :value     (inc (get-in ctx [:counter :value] 0))})
                   :deps {:counter
                          (fn [_ctx cmd]
                            {:query-id :get-by-id
                             :id       (:id cmd)})})

      (edd/reg-query :get-by-id common/get-by-id)
      (edd/reg-event :inced (fn [ctx event]
                              {:value (:value event)}))))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))
(pp ctx)

(clear!)
(exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                :id cmd-id}
                               {:cmd-id :inc
                                :id cmd-id}
                               {:cmd-id :inc
                                :id cmd-id}]})

(pp (:event-store @state/*dal-state*))
(pp (:aggregate-store @state/*dal-state*))
(pp @state/*dal-state*)

;(pp @r/*request*)

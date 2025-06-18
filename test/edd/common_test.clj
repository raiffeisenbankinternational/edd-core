(ns edd.common-test
  (:require
   [clojure.test :refer :all]
   [edd.common :as common]
   [edd.core :as edd]
   [edd.test.fixture.dal :refer [apply-cmd ctx with-mock-dal]]
   [lambda.uuid :as uuid])
  (:import
   (clojure.lang ExceptionInfo)))

(deftest test-parse-param
  (is (= "simple-test"
         (common/parse-param "simple-test")))
  (is (= 45
         (common/parse-param 45)))
  (is (= "sa"
         (common/parse-param {:query-id ""
                              :id       "sa"})))

  (is (= 45
         (common/parse-param {:query-id ""
                              :sequence 45})))
  (is (thrown?
       ExceptionInfo
       (common/parse-param {:query-id ""
                            :sequence 45
                            :wong     :attr})))
  (let [id (uuid/gen)]
    (is (= id
           (common/parse-param {:query-id " "
                                :name     id})))
    (is (= id
           (common/parse-param id)))))

(deftest get-by-id-and-version-test
  (with-mock-dal
    (let [aggregate-id (uuid/gen)
          test-cmd (fn [attrs] {:cmd-id :test-cmd
                                :id aggregate-id
                                :attrs attrs})
          ctx (-> ctx
                  (edd/reg-event :aggregate-created (fn [_ event]
                                                      {:id (:id event)
                                                       :attrs (:attrs event)}))
                  (edd/reg-cmd :test-cmd (fn [_ cmd]
                                           {:event-id :aggregate-created
                                            :id (:id cmd)
                                            :attrs (:attrs cmd)})))]
      (apply-cmd ctx (test-cmd {:val 1}))
      (apply-cmd ctx (test-cmd {:val 2}))
      (testing "version 1"
        (is (= {:id aggregate-id
                :attrs {:val 1}
                :version 1}
               (common/get-by-id-and-version ctx {:id aggregate-id :version 1}))))

      (testing "version 2"
        (is (= {:id aggregate-id
                :attrs {:val 2}
                :version 2}
               (common/get-by-id-and-version ctx {:id aggregate-id :version 2}))))

      (testing "version 3 = does not exist yet"
        (is (thrown? ExceptionInfo
                     (common/get-by-id-and-version ctx {:id aggregate-id :version 3})))))))


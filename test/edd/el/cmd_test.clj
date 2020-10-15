(ns edd.el.cmd-test
  (:require [clojure.test :refer :all]
            [edd.el.cmd :as el-cmd]))


(deftest test-empty-commands-list
  (let [ctx {:req {}}]
    (is (= (assoc ctx :error {:commands :empty})
           (el-cmd/validate-commands ctx)))))

(deftest test-invalid-command
  (let [ctx {:commands [{}]}]
    (is (= (assoc ctx :error {:spec [{:cmd-id ["missing required key"]}]})
           (el-cmd/validate-commands ctx)))))

(deftest test-invalid-cmd-id-type
  (let [ctx {:commands [{:cmd-id "wrong"}]}]
    (is (= (assoc ctx :error {:spec [{:cmd-id ["should be a keyword"]}]})
           (el-cmd/validate-commands ctx))))
  (let [ctx {:commands [{:cmd-id :test}]}]
    (is (= ctx
           (el-cmd/validate-commands ctx)))))


(deftest test-custom-schema
  (let [ctx {:spec {:test [:map [:name string?]]}}
        cmd-missing (assoc ctx :commands [{:cmd-id :test}])
        cmd-invalid (assoc ctx :commands [{:cmd-id :test
                                           :name   :wrong}])
        cmd-valid (assoc ctx :commands [{:cmd-id :test
                                         :name   "name"}])]

    (is (= (assoc cmd-missing :error {:spec [{:name ["missing required key"]}]})
           (el-cmd/validate-commands cmd-missing)))

    (is (= (assoc cmd-invalid :error {:spec [{:name ["should be a string"]}]})
           (el-cmd/validate-commands cmd-invalid)))
    (is (= cmd-valid
           (el-cmd/validate-commands cmd-valid)))))
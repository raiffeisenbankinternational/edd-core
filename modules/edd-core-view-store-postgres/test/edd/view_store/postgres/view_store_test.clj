(ns edd.view-store.postgres.view-store-test
  "
  Mostly to ensure the target namespace compiles.
  "
  (:require
   [clojure.test :refer [deftest is]]
   [edd.view-store.postgres.view-store :as view-store]))

(deftest test-register
  (is (= {:view-store :postgres}
         (view-store/register {}))))

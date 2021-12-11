(ns edd.modules.import.cmd-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [sdk.aws.s3 :as s3]
            [edd.test.fixture.dal :as mock]
            [edd.modules.import.module :as module]
            [edd.modules.import.util :as util]
            [lambda.uuid :as uuid]))

(def ctx
  (-> mock/ctx
      (module/register :files [:file1 :file2])
      ))

(deftest test-import
  (with-redefs [s3/get-object (fn [_ object]
                                (util/to-csv
                                  [{:COL1 "val1"}]))]
    (let [date "2021-12-06"
          id (uuid/named date)
          ctx (edd/reg-event-fx ctx module/upload-done-event
                                (fn [{:keys [files]} evt]
                                  (is (= {:file1 '({:COL1 "val1"})
                                          :file2 '({:COL1 "val1"})}
                                         files))
                                  []))]
      (mock/with-mock-dal

        (mock/apply-cmd ctx {:cmd-id :object-uploaded
                             :bucket "some-bucket"
                             :date   date
                             :id     (uuid/gen)
                             :key    "prefix.some-file1.csv"})
        (mock/verify-state :event-store
                           [{:event-id  :import->file-uploaded
                             :event-seq 1
                             :id        id
                             :import    {:bucket "some-bucket"
                                         :files  {:file1 "prefix.some-file1.csv"}}
                             :meta      {}}])

        (mock/verify-state :command-store [])
        (mock/apply-cmd ctx {:cmd-id :object-uploaded
                             :bucket "some-bucket"
                             :date   date
                             :id     (uuid/gen)
                             :key    "file2.csv"})

        (mock/apply-cmd ctx {:cmd-id :object-uploaded
                             :bucket "some-bucket"
                             :date   date
                             :id     id
                             :key    "file3.csv"})


        (mock/verify-state :aggregate-store
                           [{:id      id
                             :import  {:bucket "some-bucket"
                                       :files  {:file1 "prefix.some-file1.csv"
                                                :file2 "file2.csv"}
                                       :status :uploaded}
                             :version 3}])

        (mock/verify-state :command-store [{:commands [{:cmd-id :import->start-import
                                                        :import {:bucket "some-bucket"
                                                                 :files  {:file1 "prefix.some-file1.csv"
                                                                          :file2 "file2.csv"}
                                                                 :status :uploaded}
                                                        :id     id}]
                                            :meta     {}
                                            :service  nil}])
        (mock/execute-fx-apply ctx)
        (mock/verify-state :aggregate-store [{:id      id
                                              :import  {:bucket "some-bucket"
                                                        :files  {:file1 "prefix.some-file1.csv"
                                                                 :file2 "file2.csv"}
                                                        :status :done}
                                              :version 4}])))))

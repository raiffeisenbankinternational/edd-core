(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "target/classes")

(defn uber
  "Builds a Lambda uberjar. The main namespace is AOT-compiled so the
   `lambda.Handler` gen-class (emitted by edd.java-lambda-runtime.core/start)
   ends up in the jar. Invoke as:
     clojure -T:build uber :main ping.main :artifact ping-svc"
  [{:keys [main artifact]}]
  (let [basis (b/create-basis {:project "deps.edn"})
        uber-file (format "target/%s.jar" artifact)]
    (b/delete {:path "target"})
    (b/copy-dir {:src-dirs ["src" "resources"]
                 :target-dir class-dir})
    (b/compile-clj {:basis basis
                    :ns-compile [(symbol (str main))]
                    :class-dir class-dir})
    (b/uber {:class-dir class-dir
             :uber-file uber-file
             :basis basis})
    (println "Built uberjar:" uber-file)))

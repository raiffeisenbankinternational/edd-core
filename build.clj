(ns build
  (:require
   [clojure.tools.deps.util.dir :as dir]
   [clojure.tools.deps :as deps]
   [clojure.java.io :as jio]
   [clojure.tools.build.api :as b]
   [clojure.string :as str]))

(println "edd-core.build: Building via clojure.tools.build with Clojure version" (clojure-version))

(shutdown-agents)

(def VERSION_OVERRIDE
  (System/getProperty "edd-core.override"))

(defn override-deps-with-static-build-id
  [deps app-group-id]
  (reduce-kv
   (fn [acc k v]
     (assoc acc k (if (and (str/includes? (str k) (str app-group-id))
                           (contains? v :local/root)
                           (some? VERSION_OVERRIDE))
                    {:mvn/version VERSION_OVERRIDE}
                    v)))
   deps
   deps))

(def project-deps
  (delay
   (-> "deps.edn" jio/file dir/canonicalize deps/slurp-deps)))

(defn- basis
  [app-group-id]
  (b/create-basis
   {:project (update @project-deps :deps override-deps-with-static-build-id app-group-id)
    :user :standard}))

(defn params->str
  [params]
  (pr-str params))

(defn clean [_]
  (b/delete {:path "target"}))

(defn- shared-props
  [{:keys [app-group-id app-artifact-id app-version out]}]
  (let [lib (symbol (format "%s/%s" app-group-id app-artifact-id))
        jar-file (format "%s/%s-%s.jar" out (name app-artifact-id) app-version)
        class-dir (format "%s/classes" out)
        src-dirs (-> (slurp "deps.edn")
                     read-string
                     :paths)]
    {:lib           lib
     :version       (str app-version)
     :jar-file      jar-file
     :basis         (basis app-group-id)
     :class-dir     class-dir
     :resource-dirs ["resources"]
     :target        "./"
     :src-dirs      src-dirs}))

(defn jar [{:keys [app-group-id app-artifact-id app-version out]
            :or {out "target"
                 app-version "1.0.0"}
            :as params}]
  (clean nil)
  (println "[Jar] edd-core.build: (0/3) Building jar with params:" (params->str params))
  (let [{:keys [src-dirs class-dir] :as shared-props} (shared-props params)]
    (println "[Jar] edd-core.build: (1/3) 1. Producing POM...")
    (b/write-pom (dissoc shared-props :class-dir))
    (println "[Jar] edd-core.build: (1/3) 1. Producing POM done...")
    (println "[Jar] edd-core.build: (2/3) 2. Copying sources to target...")
    ;; ~Karol Wojcik~: We don't compile anything for flat jar, since it's up to the consumer
    ;; of the library to compile the `jar` with the right classpath.
    (b/copy-dir {:src-dirs (vec (set (concat src-dirs ["resources" "src"])))
                 :target-dir class-dir})
    (println "[Jar] edd-core.build: (2/3) 2. Copying sources to target done...")
    (println "[Jar] edd-core.build: (3/3) 3. Producing flatjar file...")
    (b/jar shared-props)
    (println "[Jar] edd-core.build: (3/3) 3. Producing flatjar file done...")))

(defn install
  [{:keys [app-group-id app-artifact-id app-version out]
    :or {out "target"
         app-version "1.0.0"}
    :as params}]
  (println "[install] edd-core.build: (0/1) Installing jar with params:" (params->str params) "to local ~/.m2")
  (b/install (shared-props params))
  (println "[install] edd-core.build: (1/1) Installing jar with params:" (params->str params) "to local ~/.m2"))

(defn jar+install
  [{:keys [app-group-id app-artifact-id app-version out]
    :or {out "target"
         app-version "1.0.0"}
    :as params}]
  (let [params (assoc params :out out :app-version app-version)]
    (jar params)
    (install params)))

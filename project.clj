(defproject coggy/neocons "1.0.1-003"
  :description "Neocons is an experimental idiomatic Clojure client for the Neo4J REST API"  
  :license {:name "Eclipse Public License"}
  :min-lein-version "2.0.0"  
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.2"]
                 [clj-http "0.3.3"]
                 [clojurewerkz/support "0.1.0-beta2"]]
  :test-selectors {:default        (fn [v] (not (:time-consuming v)))
                   :time-consuming (fn [v] (:time-consuming v))
                   :focus          (fn [v] (:focus v))
                   :indexing       (fn [v] (:indexing v))
                   :cypher         (fn [v] (:cypher v))
                   :all            (fn [_] true)}
  :source-paths ["src/clojure"]
  :profiles {:1.4 {:dependencies [[org.clojure/clojure "1.4.0-beta5"]]}}
  :aliases { "all" ["with-profile" "dev:dev,1.4"] }
  :repositories {"clojure-releases" "http://build.clojure.org/releases",
                 "sonatype" {:url "http://oss.sonatype.org/content/repositories/releases",
                  :snapshots false,
                  :releases {:checksum :fail, :update :always}}}
  :java-source-paths ["src/java"]
  :warn-on-reflection true)

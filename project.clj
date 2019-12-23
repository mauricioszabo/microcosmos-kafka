(defproject microcosmos/kafka "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [fundingcircle/jackdaw "0.6.9"]]
  :profiles {:dev {:dependencies [[microcosmos "0.2.1-SNAPSHOT"]
                                  [check "0.1.0-SNAPSHOT"]]}})

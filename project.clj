(defproject kafka-nabo-service "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [clj-kafka "0.3.2"]
                 [com.taoensso/nippy "2.10.0"]
                 [org.clojure/data.csv "0.1.3"]
                 [cc.qbits/alia "2.10.0"]
                 [cc.qbits/hayt "3.0.0-rc2"]]
  :main kafka-nabo-service.core)

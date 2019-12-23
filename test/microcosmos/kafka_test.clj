(ns microcosmos.kafka-test
  (:require [clojure.test :refer [deftest testing]]
            [microcosmos.core :as components]
            [microcosmos.kafka :as kafka]
            [microcosmos.future :as future]
            [check.core :refer [check]]
            [check.async :refer [async-test]]
            [jackdaw.client :as jack-client]))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "acks" "all"
   "client.id" "foo"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(defn- send-message! []
  (with-open [producer (jack-client/producer producer-config)]
    (jack-client/produce! producer {:topic-name "test"} "{\"hello\":\"world!\"}")))

(defn- deliver-result [promise]
  (fn [f-msg :keys [output]]
    (future/intercept (fn [msg]
                        (deliver promise msg)))))

(deftest kafka-consumer
  (let [test-topic (kafka/topic "test" {:host "localhost:9092"
                                        :group-id "grp1"})
        output-topic (kafka/topic "out" {:host "localhost:9092"
                                         :group-id "grp1"})
        sub (components/subscribe-with :test test-topic
                                       :output output-topic)
        promise (promise)]

    (sub :test (deliver-result promise))))

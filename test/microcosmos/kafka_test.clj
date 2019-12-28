(ns microcosmos.kafka-test
  (:require [clojure.test :refer [deftest testing]]
            [microcosmos.core :as components]
            [microcosmos.kafka :as kafka]
            [microcosmos.future :as future]
            [check.core :refer [check]]
            [check.async :refer [async-test await!]]
            [jackdaw.client :as jack-client]
            [clojure.core.async :as async]))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "acks" "all"
   ; "client.id" "foo"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(defn- send-message! []
  (with-open [producer (jack-client/producer producer-config)]
    @(jack-client/produce! producer {:topic-name "test"} "{\"hello\":\"world!\"}")))

(deftest kafka-consumer
  (async-test "Consuming kafka messages" {:teardown (kafka/disconnect-all!)}
    (let [test-topic (kafka/topic "test" {:host "localhost:9092"
                                          :group-id "grp1"})
          sub (components/subscribe-with :test test-topic)
          chan (async/promise-chan)]

      (sub :test (fn [f-msg _] (future/intercept #(async/put! chan %) f-msg)))
      (send-message!)
      (check (await! chan) => {:payload {:hello "world!"}}))))

#_
(deftest kafka-seder
  (async-test "Consuming kafka, sending to other topics"
    {:teardown (kafka/disconnect-all!)}
    (let [test-topic (kafka/topic "test" {:host "localhost:9092" :group-id "grp1"})
          out (kafka/topic "out" {:host "localhost:9092"})
          sub (components/subscribe-with :test test-topic)
          chan (async/promise-chan)]

      (sub :test (deliver-result chan))
      (send-message!)
      (check (await! chan) => {:payload {:hello "world!"}}))))


#_
(clojure.test/run-tests)

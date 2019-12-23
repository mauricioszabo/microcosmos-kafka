(ns microcosmos.kafka
  (:require [jackdaw.client :as jack-client]
            [jackdaw.client.log :as log]))

(defn- gen-consumer-config [server group-id]
  {"bootstrap.servers" server
   "group.id" group-id
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

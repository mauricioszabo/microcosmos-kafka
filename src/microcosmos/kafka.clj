(ns microcosmos.kafka
  (:require [jackdaw.client :as jack-client]
            [jackdaw.client.log :as jack-log]
            [microcosmos.io :as io]
            [microcosmos.future :as future]
            [microcosmos.logging :as log]
            [clojure.core.async :as async])
  (:import [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition]))

(defn- gen-consumer-config [server group-id]
  {"bootstrap.servers" server
   "group.id" group-id
   "enable.auto.commit" false
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(def ^:private conns (atom {}))

(comment
  (def opts {:host "localhost:9092" :group-id "grp1"})
  (def topic-name "test")
  (with-open [consumer (-> (gen-consumer-config (:host opts) (:group-id opts))
                           (jack-client/consumer)
                           (jack-client/subscribe [{:topic-name topic-name}]))]
    ; (swap! conns assoc topic-name {:consumer consumer
    ;                                :future (future)})
    (doseq [val (jack-log/log consumer (:ms-timeout opts 100))]
      (prn :processing? val))))


(defrecord Topic [topic-name opts]
  io/IO
  (listen [self function]
    (let [consumer (-> (gen-consumer-config (:host opts) (:group-id opts))
                       (jack-client/consumer)
                       (jack-client/subscribe [{:topic-name topic-name}]))
          handler (future
                   (binding [future/execute* future/sync-execute]
                     (doseq [val (jack-log/log consumer (:ms-timeout opts 100))]
                       (function {:meta (dissoc val :value)
                                  :payload (-> val :value io/deserialize-msg)}))))]

      (swap! conns assoc topic-name {:consumer consumer
                                     :future handler})))

  ; (send! [_ {:keys [payload meta] :or {meta {}}}]
  ;        (when-not cid (raise-error))
  ;        (let [payload (io/serialize-msg payload)
  ;              meta (assoc meta :headers (normalize-headers (assoc meta :cid cid)))]
  ;          (basic/publish channel name "" payload meta)))
  ;
  (ack! [_ {:keys [meta]}]
    (let [consumer (get-in @conns [topic-name :consumer])
          topic-partition (new TopicPartition topic-name (:partition meta))
          offset (new OffsetAndMetadata (-> meta :offset inc))]
      (.commitSync consumer {topic-partition offset})))

  (log-message [_ logger {:keys [payload meta]}]
    (let [meta (assoc meta :queue name)]
      (log/info logger "Processing message"
                :payload (io/serialize-msg payload)
                :meta (io/serialize-msg meta)))))

  ; (reject! [self msg _]
  ;          (let [meta (:meta msg)
  ;                meta (assoc meta :headers (normalize-headers meta))
  ;                payload (-> msg :payload io/serialize-msg)]
  ;            (reject-or-requeue self meta payload))))
  ;
  ; health/Healthcheck
  ; (unhealthy? [_] (when (core/closed? channel)
  ;                   {:channel "is closed"})))
  ;

(defn topic [^String topic-name opts]
  (fn [ & args]
      (->Topic topic-name opts)))

(defn disconnect-all! []
  (doseq [[topic-name {:keys [consumer future chan]}] @conns]
    (.cancel future true)
    (Thread/sleep 100)
    (.close consumer)
    (swap! conns dissoc topic-name)))

(ns kafka-nabo-service.core
  (:require [clj-kafka.consumer.zk :as cz]
            [clj-kafka.core :as c]
            [clj-kafka.new.producer :as p]
            [taoensso.nippy :as n]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt])
  (:import (kafka.consumer KafkaStream)
           (java.math BigDecimal)))

(def config {"zookeeper.connect" "localhost:2181"
             "group.id" "clj-kafka.consumer"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})

(defn produce [msg topic]
  (with-open [p (p/producer {"bootstrap.servers" "127.0.0.1:9092"} (p/byte-array-serializer) (p/byte-array-serializer))]
    (p/send p (p/record topic (n/freeze msg {:compressor nil})))))

(defrecord KafkaMessage [topic offset partition key value-bytes])

(defn- find-naboer [vur-ejd]
  (prn "Find naboer for " vur-ejd)
  {:vur-ejd-id (:vur-ejd-id vur-ejd)
   :naboer [{:vur-ejd-id 91829 :vaegt 0.1} {:vur-ejd-id 90912019 :vaegt 0.2} {:vur-ejd-id 12912 :vaegt 0.2} {:vur-ejd-id 912012 :vaegt 0.15} {:vur-ejd-id 812398 :vaegt 0.05} {:vur-ejd-id 892319 :vaegt 0.05} {:vur-ejd-id 973282 :vaegt 0.06} {:vur-ejd-id 713929 :vaegt 0.19}]})

(defn -main
  "Laeser ejendomme der skal vurderes, finder naboerne, og poster dem i naboer topic"
  [group-id-number]
  (c/with-resource [c (cz/consumer (assoc config "group.id" (str "nabo-" group-id-number)))]
    cz/shutdown
    (let [stream (cz/create-message-stream c "vurder2")
          it (.iterator ^KafkaStream stream)]
      (while (.hasNext it)
        (as-> (.next it) msg
              (KafkaMessage. (.topic msg) (.offset msg) (.partition msg) (.key msg) (.message msg))
              (find-naboer (n/thaw (:value-bytes msg)))
              (produce msg "naboer"))))))

(def km-mellem-grader 111.16)

(def syd 54.5641)

(def nord 57.7486)

(def vest 8.075)

(def oest 15.1998)

;;Denmark lowerleft(lon,lat) = 8.075 , 54,564167
;;Denmark upperright(lon,lat) = 15.19972 , 57.74861

(defn find-region [long lat]
  (let [dx (* km-mellem-grader (- long vest))
        dy (* km-mellem-grader (- lat syd))
      ;  _ (prn dx ", " dy)
        long-min (+ vest (/ (Math/floor dx) km-mellem-grader))
        long-max (+ vest (/ (Math/ceil dx) km-mellem-grader))
        lat-min (+ syd (/ (Math/floor dy) km-mellem-grader))
        lat-max (+ syd (/ (Math/ceil dy) km-mellem-grader))]
    {:left-lower [long-min lat-min]
     :right-upper [long-max lat-max]}))

(defn find-naermeste-regioner [region]
  (let [left-lower (:left-lower region)
        right-upper (:right-upper region)
        left-upper-region (find-region (- (first left-lower) 0.003) (+ (second right-upper) 0.003))
        left-medium-region (find-region (- (first left-lower) 0.003) (+ (second left-lower) 0.003))
        left-lower-region (find-region (- (first left-lower) 0.003) (- (second left-lower) 0.003))
        medium-upper-region (find-region (+ (first left-lower) 0.003) (+ (second right-upper) 0.003))
        medium-lower-region (find-region (+ (first left-lower) 0.003) (- (second left-lower) 0.003))
        right-upper-region (find-region (+ (first right-upper) 0.003) (+ (second right-upper) 0.003))
        right-medium-region (find-region (+ (first right-upper) 0.003) (+ (second left-lower) 0.003))
        right-lower-region (find-region (+ (first right-upper) 0.003) (- (second left-lower) 0.003))]
    [left-upper-region left-medium-region left-lower-region medium-upper-region medium-lower-region right-upper-region right-medium-region right-lower-region region]))

(comment CREATE TYPE point (
                                lat double,
                                    long double
                                    );

        CREATE TYPE region (
                                lowleft frozen<point>,
                                        highright frozen<point>
                                        );

        CREATE TABLE salg (
                               region frozen<region>,
                                      reg_tid timestamp,
                                      ejd_kvm bigint,
                                      grund_kvm bigint,
                                      lat double,
                                      long double,
                                      salgspris bigint,
                                      vur_ejd_id bigint,
                                      PRIMARY KEY (region, vur_ejd_id, reg_tid)
                                      );
        )

(def cluster (alia/cluster {:contact-points ["localhost"]}))

(def session (alia/connect cluster))

(def ->region (alia/udt-encoder session "ice" "region"))

(def ->point (alia/udt-encoder session "ice" "point"))

(defn konverter-region-til-cassandra-format [region]
  (->region {:lowleft (->point {:lat (second (:left-lower region)) :long (first (:left-lower region))})
             :highright (->point {:lat (second (:right-upper region)) :long (first (:right-upper region))})}))

(defn insert-row [r]
  (let [region (find-region (Double/parseDouble (r 3)) (Double/parseDouble (r 4)))
        data-region (konverter-region-til-cassandra-format region)]
    (alia/execute session
                  (hayt/queries
                   (hayt/insert :ice.salg (hayt/values [[:region data-region] [:reg_tid (r 6)] [:ejd_kvm (Long/parseLong (r 1))] [:grund_kvm (Long/parseLong (r 2))] [:lat (Double/parseDouble (r 4))] [:long (Double/parseDouble (r 3))] [:salgspris (Long/parseLong (r 5))] [:vur_ejd_id (Long/parseLong (apply str (rest (r 0))))]]))))))

(defn insert-file [f]
  (with-open [in-file (io/reader f)]
    (let [input (csv/read-csv in-file :separator \;)]
      (doall (pmap #(insert-row %) input)))))

(defn konverter-til-cassandra-format [regioner]
  (prn regioner)
  (mapv #(konverter-region-til-cassandra-format %) regioner))

(defn select-region [region]
  (alia/execute session
                (hayt/select :ice.salg (hayt/where {:region region}))))

(defn find-alle-salg [long lat]
  (let [regioner (-> (find-region long lat)
                     find-naermeste-regioner
                     konverter-til-cassandra-format)]
    (flatten (doall (pmap #(select-region %) regioner)))))

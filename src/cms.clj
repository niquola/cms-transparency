(ns cms
  (:require [cheshire.core]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(def data (cheshire.core/parse-stream (io/reader (io/input-stream (io/file"data.json"))) keyword))

(def header (dissoc data :in_network))

(def columns
  (-> (for [item (take 1 (:in_network data))]
     (for [rates (:negotiated_rates item)]
       (for [prices (:negotiated_prices rates)]
         (for [svc (:service_code prices)]
           (for [provider (:provider_groups rates)]
             (for [npi (:npi provider)]
               (merge
                header
                (dissoc item :negotiated_rates)
                (dissoc rates :negotiated_prices :provider_groups)
                (dissoc prices :service_code)
                (dissoc provider :npi :tin)
                {:service_code svc}
                {:npi svc (keyword (str "tin_" (get-in provider [:tin :type]))) (get-in provider [:tin :value])})))))))
      first
      first
      first
      first
      first
      first
      keys))

(with-open [w (io/writer "data.ndjson")]
  (doseq [item (:in_network data)]
    (doseq [rates (:negotiated_rates item)]
      (doseq [prices (:negotiated_prices rates)]
        (doseq [svc (:service_code prices)]
          (doseq [provider (:provider_groups rates)]
            (doseq [npi (:npi provider)]
              (.write  w
               (cheshire.core/generate-string
                (merge
                 header
                 (dissoc item :negotiated_rates)
                 (dissoc rates :negotiated_prices :provider_groups)
                 (dissoc prices :service_code)
                 (dissoc provider :npi :tin)
                 {:service_code svc}
                 {:npi svc (keyword (str "tin_" (get-in provider [:tin :type]))) (get-in provider [:tin :value])})))
              (.write w "\n"))))))))


(with-open [w (io/writer "data.tsv")]
  (.write w (str/join  "\t" (mapv name columns)))
  (doseq [item (:in_network data)]
    (doseq [rates (:negotiated_rates item)]
      (doseq [prices (:negotiated_prices rates)]
        (doseq [svc (:service_code prices)]
          (doseq [provider (:provider_groups rates)]
            (doseq [npi (:npi provider)]
              (let [item (merge
                          header
                          (dissoc item :negotiated_rates)
                          (dissoc rates :negotiated_prices :provider_groups)
                          (dissoc prices :service_code)
                          (dissoc provider :npi :tin)
                          {:service_code svc}
                          {:npi svc (keyword (str "tin_" (get-in provider [:tin :type]))) (get-in provider [:tin :value])})
                    row (->> columns (mapv (fn [k] (get item k))))]
                (.write w (str/join  "\t" row)))
              (.write w "\n"))))))))

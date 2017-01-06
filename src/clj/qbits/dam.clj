(ns qbits.dam
  "Direct port of
  https://gist.githubusercontent.com/josiahcarlson/3cb0561707fd2ca1f176/raw/08ab61b1363d2b1ec8426dafea976f8414b392b0/rate_limit.py

  rate_limit.py
  Written May 7-8, 2014 by Josiah Carlson
  Released under the MIT license.

  TODO: preloading of script in redis instances (evalsha) + proper
  script recovery in case of server reboot"
  (:require
   [clojure.java.io :as io]
   [taoensso.carmine :as car]))

(defn now []
  (-> (java.util.Date.)
      .getTime
      (/ 1000)
      long))

(def over-limit-lua
  (delay (slurp (io/resource "over_limit.lua"))))

(def over-limit-sliding-lua
  (delay (slurp (io/resource "over_limit_sliding.lua"))))

(defn over-limit-basic?
  "Will return whether the count referenced by key has gone over its limit.
   This method uses a fixed schedule where limits are reset at the top of the
   time period.
   On an Intel i7-4770 with 5 clients, you can expect roughly 90k calls/second
   with Redis 2.8.8 (no pipelining, more clients can push this to over 200k
   calls/second).
   Note: you probably want over_limit() or over_limit_sliding() instead.
   Arguments:
       conn - a Redis connection object
       key - a client identification key from which the time-based key will be
             derived
       limit - the number of times that the client can call without going over
               their limit
       duration - how long before we can reset the current request count,
                  defaults to 1 minute (you can pass SECOND, MINUTE, HOUR, or
                  DAY for different time spans)"
  [conn key limit duration]
  (let [now (now)
        key (str key ":" (long (/ now duration)))
        [cnt _] (car/wcar conn
                          (car/incr key)
                          (car/expire key
                                      (inc (long (- duration
                                                    (mod now duration))))))]
    (> cnt limit)))

(defn over-limit?
  "Will return whether the caller is over any of their limits. Uses a fixed
   schedule where limits are reset at the top of each time period.

   On an Intel i7-4770 with 5 clients, you can expect roughly 60k calls/second
   with Redis 2.8.8 (no pipelining).
   Arguments:
      conn - a Redis connection object
      base_keys - how you want to identify the caller, pass a list of
                   identifiers
       second, minute, hour, day - limits for each resolution
       weight - how much does this \"call\" count for"
  [conn base-keys {:keys [second minute hour day weight]
                   :or {second 0
                        minute 0
                        hour 0
                        day 0
                        weight 1}}]
  (let [args (concat [@over-limit-lua (count base-keys)]
                     base-keys
                     [second minute hour day weight (now)])]
    (= 1 (car/wcar conn (apply car/eval args)))))

(defn over-limit-sliding?
  "Will return whether the caller is over any of their limits. Uses a sliding
   schedule with millisecond resolution
   On an Intel i7-4770 with 5 clients, you can expect roughly 52k calls/second
   with Redis 2.8.8 (no pipelining).
   Arguments:
       conn - a Redis connection object
       base-keys - how you want to identify the caller, pass a list of
                   identifiers
       second, minute, hour, day - limits for each resolution
       weight - how much does this call count for"
  [conn base-keys {:keys [minute hour day weight]
                   :or {minute 0
                        hour 0
                        day 0
                        weight 1}}]
  (let [args (concat [@over-limit-sliding-lua (count base-keys)]
                     base-keys
                     [minute hour day weight (now)])]
    (= 1 (car/wcar conn (apply car/eval args)))))

;; (def conn-test
;;   {:pool {}
;;    :spec {:host "localhost"
;;           :port 6380
;;           :db 0}})
;; (prn (over-limit? conn-test ["foo"] {:second 1}))
;; (prn (over-limit-sliding? conn-test ["foo"] {:minute 1}))

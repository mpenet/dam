(defproject cc.qbits/dam "0.1.0"
  :description "Redis/LUA backed rate limiter"
  :url "https://github.com/mpenet/dam"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [com.taoensso/carmine "2.12.2-private"]]
  :source-paths ["src/clj"]
  :global-vars {*warn-on-reflection* true})

How to use
===================
1. Start local cluster, 
  ```
  bin/local -ip 127.0.0.1 -port 3000
  ```
2. Submit the stock crawler
  ```
  bin\gear app -jar examples\gearpump-examples-assembly-0.3.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.stock.main.Stock -master 127.0.0.1:3000
  ```
  
  If you are behind  a proxy, you need to set the proxy address
  ```
  bin\gear app -jar examples\gearpump-examples-assembly-0.3.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.stock.main.Stock -master 127.0.0.1:3000 -proxy host:port
  ```
  
3. Check the UI
  http://127.0.0.1:8080  
  
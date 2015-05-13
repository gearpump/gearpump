How to use
===================
1. Start local cluster, 
  ```
  bin/local
  ```
2. Submit the stock crawler
  ```
  bin\gear app -jar examples\gearpump-examples-assembly-0.3.2-SNAPSHOT.jar gearpump.streaming.examples.stock.main.Stock
  ```
  
  If you are behind  a proxy, you need to set the proxy address
  ```
  bin\gear app -jar examples\gearpump-examples-assembly-0.3.2-SNAPSHOT.jar gearpump.streaming.examples.stock.main.Stock -proxy host:port
  ```
  
3. Check the UI
  http://127.0.0.1:8080  
  
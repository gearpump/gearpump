A more complicated dag represented by the following dot code

```
 digraph flow {
     Source_0 -> Sink_0;
     Source_0 -> Sink_1;
     Source_0 -> Sink_2;
     Source_0 -> Node_1;
     Source_1 -> Node_0;
     Node_0 -> Sink_3;
     Node_1 -> Sink_3;
     Node_1 -> Sink_4;
     Node_1 -> Node_4;
     Node_2 -> Node_3;
     Node_1 -> Node_3;
     Source_0 -> Node_2;
     Source_0 -> Node_3;
     Node_3 -> Sink_3;
     Node_4 -> Sink_3;
     Source_1 -> Sink_4;
 }
```

Topological sort is

```
[Source_0,Node_1,Node_4,Source_1,Sink_0,Node_3,Sink_1,Sink_4,Sink_2,Sink_3,Node_0,Node_2]
```

The example will be a starting point of reading an input format and programmatically generating the AppDescription,
TaskDescription and TaskActors. Currently this is hand built using the dot format syntax.  

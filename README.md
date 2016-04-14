# Dataflow
Write Data parallel programs using sequential building blocks.

### Writing a Vertex
Sequential Vertex is written by extending the AbstractVertex class and override the execute method.
```java
class MyVertex extends AbstractVertex {
      @Override
      public void execute(String line, Collector collector){
      ....
      }
}
```



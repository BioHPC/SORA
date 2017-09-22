# Composite Edge Contraction

Composite Edge Contraction(CEC) is a tool used to reduce the total number of edges by combining unneccessary eges. In order to properly use this module you need to install the dependencies. The README in the main SORA directory describes what is needed.


## Build CEC
The first step is building the package. This process is completed by sbt. Starting in the main SORA directory move to the 'CompositeEdgeContraction' on the command line.
```
> cd CompositeEdgeContraction
```
Second step is after you are in the CompositeEdgeContraction folder you can run the ```sbt``` command. This builds the package into a single fat jar with all of the secondary jar files included. 
```
> sbt assembly
```
Once you have created the jar, confirm the jar is located in
```
target/scala-2.11/Composite\ Edge\ Contraction\ Project-assembly-1.1.jar
```
## Run CEC
Finally you can call ```spark-submit``` to submit your program. Where the data.txt is your input data and the results_dir/ is the location where you want the results stored.
```
> {spark_home}/bin/spark-submit --class CompositeEdgeContraction --master local[4] target/scala-2.11/Composite\ Edge\ Contraction\ Project-assembly-1.1.jar data.txt results_dir/
```

An example using a test dataset:
```
> {spark_home}/bin/spark-submit --class CompositeEdgeContraction --master local[4] target/scala-2.11/Composite\ Edge\ Contraction\ Project-assembly-1.1.jar data/test05_edge_list.txt SORA/resultsCEC/ ~/CECResults/
```


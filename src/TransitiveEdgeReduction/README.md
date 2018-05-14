# Transitive Edge Reduction 

Transitive Edge Reduction (TRE) is a tool that can be used to remove redudant edges. In order to properly use this module you need to install the dependencies. The README in the main SORA directory describes what is needed.

## Build TRE
The first step is building the package. This process is completed by sbt. Starting in the main SORA directory move to the 'TransitiveEdgeReduction' on the command line.
```
> cd TransitiveEdgeReduction
```
Second step is after you are in the TransitiveEdgeReduction folder you can run the ```sbt``` command. This builds the package into a single jar. 
```
> sbt package
```
Once you have created the jar, confirm the jar is located in 
```
target/scala-2.11/transitive-edge-reduction-module_2.11-1.0.jar
```
## Run TRE
Finally you can call ```spark-submit``` to submit your program. Where the data.txt is your input data and the results_dir/ is the location where you want the results stored.
```
> {spark_home}/bin/spark-submit --class TransitiveEdgeReduction --master local[4] target/scala-2.11/transitive-edge-reduction-module_2.11-1.0.jar data.txt results_dir/
```

An example using a test dataset:
```
> {spark_home}/bin/spark-submit --class TransitiveEdgeReduction --master local[4] target/scala-2.11/transitive-edge-reduction-module_2.11-1.0.jar data/3reads_overlap_beforeTransitiveEdgeReduction_edge_list.txt ~/TERResults/
```

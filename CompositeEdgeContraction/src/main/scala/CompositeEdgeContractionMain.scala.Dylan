import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

//import TransitiveEdgeReduction

object CompositeEdgeContraction {

  val usage = """
    Usage: CompositeEdgeContraction filename
  """
  //prints out dot format of edge RDD
  def toDot(e: EdgeRDD[String]) {
    e.map(ed => ed.srcId.toString + " -> " + ed.dstId.toString + ";").foreach(println)
  }
  
  def CompositeEdgeContraction(graph: Graph[Int, String], debug: Int = 0) : Graph[Int, String] = {
    
    val inOutVertexRDD: VertexRDD[(Long,Long)] = graph.aggregateMessages[(Long,Long)](
      // map function: for each edge send a message to the src and dst vertices with the edge attribute
      sendMsg = { triplet => {
        val orientation = triplet.attr.split(",")(0).toInt
        if (orientation == 1) {       // 1 = u<-------->v      reverse of u to forward of v
          //Iterator((triplet.srcId, (1,0)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(1,0)
          triplet.sendToDst(1,0)
        }
        else if (orientation == 2) {  // 2 = u>--------<v      forward of u to reverse of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (0,1)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(0,1)
        }
        else if (orientation == 3) {  // 3 = u>-------->v      forward of u to forware of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(1,0)
        }
        else {
          Iterator.empty
        }
      }},
      // reduce function: sum in and out degree count
      mergeMsg = {(a, b) => (a._1+b._1, a._2+b._2)}
    )

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"inOutVertexRDD VertexRDD generated:")
      inOutVertexRDD.collect.foreach(println(_))

      // check one in and one out degree vertices
      println(s"====================================================")
      println(s"Check who has one in and one out degree:")
      inOutVertexRDD.filter {
        case (id, attr) => (attr._1 == 1 && attr._2 == 1)
      }.collect.foreach {
        case (id, attr) => println(s"Vertex ${id} has one in and out degree with properties: ${attr}")
      }
    }

    // create initial inOutGraph
    var inOutGraph: Graph[(Long,Long),String] = graph.mapVertices{ case (id,default) => (0,0) }

    // Merge the inOutVertexRDD values into the inOutGraph
    inOutGraph = inOutGraph.outerJoinVertices(inOutVertexRDD) {
       (vid, old, newOpt) => newOpt.getOrElse(old)
    }

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"inOutGraph was generated:}")
      inOutGraph.vertices.collect.foreach(println(_))
      inOutGraph.edges.collect.foreach(println(_))
    }

    // Mark the edge to be removed and contracted.
    // If destination node has (1,1) in out degrees, then mark the edge.
    // If the edge was marked, then the edge will be removed and contracted later
    val markedGraph: Graph[(Long,Long),(String, Boolean)] = inOutGraph.mapTriplets(
      triplet => if (triplet.dstAttr._1 == 1 && triplet.dstAttr._2 == 1) (triplet.attr, true) 
                 else (triplet.attr, false)
    )

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"markedGraph was generated:}")
      markedGraph.vertices.collect.foreach(println(_))
      markedGraph.edges.collect.foreach(println(_))
    }

    // Restric graph to contractable (mark=true) edges
    val contractableGraph = markedGraph.subgraph(epred = e => e.attr._2 == true)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractableGraph was generated:}")
      contractableGraph.vertices.collect.foreach(println(_))
      contractableGraph.edges.collect.foreach(println(_))
    }

    // Compute connected component id for each vertex from the contractableGraph
    //contractableGraph.connectedComponents.edges.foreach(println)
    val conectedComponentVertices = contractableGraph.connectedComponents.vertices.cache()

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"conectedComponentVertices (connected component IDs of contractableGraph) were generated:}")
      conectedComponentVertices.collect.foreach(println(_))
    }

    // Convert the IDs of the same connected component (_._2) to be same to merge 
    val duplicateIdVertices = markedGraph.vertices.innerJoin(conectedComponentVertices) { (id, attr, cc) => (cc, attr) }.map(_._2)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"duplicateIdVertices was generated:}")
      duplicateIdVertices.collect.foreach(println(_))
    }

    // New contracted vertices after aggregating same index
    val contractedVertices: VertexRDD[(Long,Long)] = markedGraph.vertices.aggregateUsingIndex(duplicateIdVertices, (a,b) => (a._1+b._1, a._2+b._2))

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedVertices was generated:}")
      contractedVertices.collect.foreach(println(_))
    }

    // Restrict graph to remained (mark=false) edges
    val remainedGraph = markedGraph.subgraph(epred = e => e.attr._2 != true)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"remainedGraph was generated:}")
      remainedGraph.vertices.collect.foreach(println(_))
      remainedGraph.edges.collect.foreach(println(_))
    }

    // Contracted edges 
    val contractedEdges = remainedGraph.outerJoinVertices(conectedComponentVertices) { 
      (id, _, ccOpt) => ccOpt.get }
    .triplets.map { e => Edge(e.srcAttr, e.dstAttr, e.attr._1)}
        
    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedEdges was generated:}")
      contractedEdges.collect.foreach(println(_))
    }

    // Contracted graph
    val contractedGraph = Graph(contractedVertices, contractedEdges)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedGraph was generated:}")
      contractedGraph.vertices.collect.foreach(println(_))
      contractedGraph.edges.collect.foreach(println(_))
    }   

    // end composite edge contraction
    println(s"====================================================")
    println(s"Composite edge contraction done!")
/////    println(s"Number of edges of the graph after composite edge contraction: ${contractedGraph.edges.count()}")
    println(s"")
   
   // generate a list of unmdodified edges    
   val origEdges = contractedGraph.edges.intersection(graph.edges).map { e => (Edge(e.srcId,e.dstId,e.attr),1)}
   val contractedGraphEdges = contractedGraph.edges.map { e => (Edge(e.srcId,e.dstId,e.attr),1)}
 
   val markedContractedEdges = contractedGraphEdges.union(origEdges).reduceByKey(_+_).map(x => if (x._2 > 1) Edge(x._1.srcId,x._1.dstId,x._1.attr) else  Edge(x._1.srcId,x._1.dstId,x._1.attr+"$"))

   return Graph.fromEdges(markedContractedEdges,1)

  }

  def DeadEndRemoval(graph: Graph[Int, String]) : Graph[Int, String]  = {
    val inOutVertexRDD: VertexRDD[(Long,Long)] = graph.aggregateMessages[(Long,Long)](
      // map function: for each edge send a message to the src and dst vertices with the edge attribute
      sendMsg = { triplet => {
        val orientation = triplet.attr.split(",")(0).toInt
        if (orientation == 1) {       // 1 = u<-------->v      reverse of u to forward of v
          //Iterator((triplet.srcId, (1,0)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(1,0)
          triplet.sendToDst(1,0)
        }
        else if (orientation == 2) {  // 2 = u>--------<v      forward of u to reverse of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (0,1)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(0,1)
        }
        else if (orientation == 3) {  // 3 = u>-------->v      forward of u to forware of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(1,0)
        }
        else {
          Iterator.empty
        }
      }},
      // reduce function: sum in and out degree count
      mergeMsg = {(a, b) => (a._1+b._1, a._2+b._2)}
    )

    var inOutGraph: Graph[(Long,Long),String] = graph.mapVertices{ case (id,default) => (0,0) }
    inOutGraph = inOutGraph.outerJoinVertices(inOutVertexRDD) {
       (vid, old, newOpt) => newOpt.getOrElse(old)
    }

    val markedGraph: Graph[(Long,Long),(String, Boolean)] = inOutGraph.mapTriplets(
      triplet => if (triplet.dstAttr._2 == 0 && !(triplet.attr contains "$")) (triplet.attr, true) else (triplet.attr, false)
    )

    return Graph.fromEdges(markedGraph.subgraph(epred = e => e.attr._2 != true).edges.map(e => Edge(e.srcId, e.dstId, e.attr._1.replace(":$",""))),1)
  }  

  def main(args: Array[String]) {

    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }

    // if debug = 0, just print the final output
    // if debug = 1, print all intermediate results
    val debug = 0 

    // We pass the SparkContext constructor a SparkConf object which contains 
    // information about our application
    val conf = new SparkConf().setAppName("Composite Edge Contraction Module")
    // Initialize a SparkContext as part of the program
    val sc = new SparkContext(conf)

    // Intialize edgeListFile
    // val edgeListFile = "data/test05_edge_list.txt"
    val edgeListFile = args(0)

    // Load edge list into graph
    val edges = sc.textFile(edgeListFile).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          None
        } else {
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          val attr  = lineArray(2)
          // edge has 9 attributes Ex) 3,F,33,0,0,2,34,0,32
          // Col1: overlap orientation
          // 0 = u<--------<v      reverse of u to reverse of v  
          //   => This case is handled in DOT file preprocessing step and changed to 3 (u>-->v)
          // 1 = u<-------->v      reverse of u to forward of v
          // 2 = u>--------<v      forward of u to reverse of v
          // 3 = u>-------->v      forward of u to forware of v
          // Col2: overlap property F:forward, 
          //                        FRC::read1 overlaps with the reverse complement of read2
          // Col3~9: overlap length, substitutions, edits, start1, stop1, start2, stop2
          // Properties (String, Boolean)
          Some(Edge(srcId, dstId, attr+":"))
        }
      } else {
        None
      }
    }

    // construct graph from edges of DOT file
    val dotGraph = Graph.fromEdges(edges, 1)
    
    //toDot(dotGraph.edges)

    // start composite edge contraction
   println(s"====================================================")
   println(s"Composite edge contraction started!")
////   println(s"Number of edges of the graph before composite edge contraction: ${dotGraph.edges.count()}")
   println(s"")

    // debug
    if (debug > 0) {
      // print vertices and edges
      println(s"====================================================")
      println(s"dotGraph vertices:")
      dotGraph.vertices.collect.foreach(println(_))
      println(s"====================================================")
      println(s"dotGraph edges:")
      dotGraph.edges.collect.foreach(println(_))

      // the Edge class has srcId, dstId, attr members
      println(s"====================================================")
      println(s"dotGraph edges orientation info:")
      for (e <- dotGraph.edges.collect) {
        println(s"${e.srcId} --> ${e.dstId} has ${e.attr} edge properties")
      }
    }

    // in and out degree count and save to inOutVertexRDD (read orientation degree into in(Long), out(Long))
    val inOutVertexRDD: VertexRDD[(Long,Long)] = dotGraph.aggregateMessages[(Long,Long)](
      // map function: for each edge send a message to the src and dst vertices with the edge attribute
      sendMsg = { triplet => {
        val orientation = triplet.attr.split(",")(0).toInt
        if (orientation == 1) {       // 1 = u<-------->v      reverse of u to forward of v
          //Iterator((triplet.srcId, (1,0)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(1,0)
          triplet.sendToDst(1,0)
        }
        else if (orientation == 2) {  // 2 = u>--------<v      forward of u to reverse of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (0,1)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(0,1)
        }
        else if (orientation == 3) {  // 3 = u>-------->v      forward of u to forware of v
          //Iterator((triplet.srcId, (0,1)), (triplet.dstId, (1,0)))
          triplet.sendToSrc(0,1)
          triplet.sendToDst(1,0)
        }
        else {
          Iterator.empty
        }
      }},
      // reduce function: sum in and out degree count
      mergeMsg = {(a, b) => (a._1+b._1, a._2+b._2)}
    )

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"inOutVertexRDD VertexRDD generated:")
      inOutVertexRDD.collect.foreach(println(_))

      // check one in and one out degree vertices
      println(s"====================================================")
      println(s"Check who has one in and one out degree:")
      inOutVertexRDD.filter {
        case (id, attr) => (attr._1 == 1 && attr._2 == 1)
      }.collect.foreach {
        case (id, attr) => println(s"Vertex ${id} has one in and out degree with properties: ${attr}")
      }
    }

    // create initial inOutGraph
    var inOutGraph: Graph[(Long,Long),String] = dotGraph.mapVertices{ case (id,default) => (0,0) }

    // Merge the inOutVertexRDD values into the inOutGraph
    inOutGraph = inOutGraph.outerJoinVertices(inOutVertexRDD) {
       (vid, old, newOpt) => newOpt.getOrElse(old)
    }

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"inOutGraph was generated:}")
      inOutGraph.vertices.collect.foreach(println(_))
      inOutGraph.edges.collect.foreach(println(_))
    }

    // Mark the edge to be removed and contracted.
    // If destination node has (1,1) in out degrees, then mark the edge.
    // If the edge was marked, then the edge will be removed and contracted later
    val markedGraph: Graph[(Long,Long),(String, Boolean)] = inOutGraph.mapTriplets(
      triplet => if (triplet.dstAttr._1 == 1 && triplet.dstAttr._2 == 1) (triplet.attr, true) 
                 else (triplet.attr, false)
    )

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"markedGraph was generated:}")
      markedGraph.vertices.collect.foreach(println(_))
      markedGraph.edges.collect.foreach(println(_))
    }

    // Restric graph to contractable (mark=true) edges
    val contractableGraph = markedGraph.subgraph(epred = e => e.attr._2 == true)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractableGraph was generated:}")
      contractableGraph.vertices.collect.foreach(println(_))
      contractableGraph.edges.collect.foreach(println(_))
    }

    // Compute connected component id for each vertex from the contractableGraph
    //contractableGraph.connectedComponents.edges.foreach(println)
    val conectedComponentVertices = contractableGraph.connectedComponents.vertices.cache()

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"conectedComponentVertices (connected component IDs of contractableGraph) were generated:}")
      conectedComponentVertices.collect.foreach(println(_))
    }

    // Convert the IDs of the same connected component (_._2) to be same to merge 
    val duplicateIdVertices = markedGraph.vertices.innerJoin(conectedComponentVertices) { (id, attr, cc) => (cc, attr) }.map(_._2)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"duplicateIdVertices was generated:}")
      duplicateIdVertices.collect.foreach(println(_))
    }

    // New contracted vertices after aggregating same index
    val contractedVertices: VertexRDD[(Long,Long)] = markedGraph.vertices.aggregateUsingIndex(duplicateIdVertices, (a,b) => (a._1+b._1, a._2+b._2))

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedVertices was generated:}")
      contractedVertices.collect.foreach(println(_))
    }

    // Restrict graph to remained (mark=false) edges
    val remainedGraph = markedGraph.subgraph(epred = e => e.attr._2 != true)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"remainedGraph was generated:}")
      remainedGraph.vertices.collect.foreach(println(_))
      remainedGraph.edges.collect.foreach(println(_))
    }

    // Contracted edges 
    val contractedEdges = remainedGraph.outerJoinVertices(conectedComponentVertices) { 
      (id, _, ccOpt) => ccOpt.get }
    .triplets.map { e => Edge(e.srcAttr, e.dstAttr, e.attr._1)}
        
    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedEdges was generated:}")
      contractedEdges.collect.foreach(println(_))
    }

    // Contracted graph
    val contractedGraph = Graph(contractedVertices, contractedEdges)

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"contractedGraph was generated:}")
      contractedGraph.vertices.collect.foreach(println(_))
      contractedGraph.edges.collect.foreach(println(_))
    }   

    // end composite edge contraction
    println(s"====================================================")
    println(s"Composite edge contraction done!")
 //   println(s"Number of edges of the graph after composite edge contraction: ${contractedGraph.edges.count()}")
    println(s"")
   
   // generate a list of unmdodified edges    
   val origEdges = contractedGraph.edges.intersection(dotGraph.edges).map { e => (Edge(e.srcId,e.dstId,e.attr),1)}
   val contractedGraphEdges = contractedGraph.edges.map { e => (Edge(e.srcId,e.dstId,e.attr),1)}
 
   val markedContractedEdges = contractedGraphEdges.union(origEdges).reduceByKey(_+_).map(x => if (x._2 > 1) Edge(x._1.srcId,x._1.dstId,x._1.attr) else  Edge(x._1.srcId,x._1.dstId,x._1.attr+"$"))

   val markedContractedGraph = Graph.fromEdges(markedContractedEdges,1)
   //toDot(markedContractedGraph.edges)

   val derGraph = DeadEndRemoval(markedContractedGraph)

   println(s"====================================================")
   println(s"Dead end removal done!")
  // println(s"Number of edges of the graph after dead end removal: ${derGraph.edges.count()}")
   println(s"")

   //derGraph.edges.foreach(println)

   val cec2Graph = CompositeEdgeContraction(derGraph)


  // cec2Graph.edges.foreach(println)  
  // cec2Graph.edges.saveAsTextFile("/student/apaul7/Research/results")
   var edgeStr = cec2Graph.edges.map(e => e.srcId.toString() + "\t" + e.dstId.toString() + "\t" + e.attr.toString().replaceAll(":",""))
  // edgeStr.saveAsTextFile("/student/apaul7/Research/results")
   edgeStr.saveAsTextFile("s3://sparkbenchmarkemr/afterCEC")
   println("File saved!") 

  }
}

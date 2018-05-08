import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._
import org.apache.spark.graphx

import scala.util.control.Breaks._
import collection.mutable.HashMap


object TransitiveEdgeReduction{

  val usage = """
    Usage: TransitiveEdgeReduction filename directory
  """

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
    val conf = new SparkConf().setAppName("Transitive Edge Reduction Module")
    // Initialize a SparkContext as part of the program
    val sc = new SparkContext(conf)

    // Intialize edgeListFile
    val edgeListFile = args(0)
    val resultDirectory = args(1)


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

    // start transitive edge reduction
    println(s"====================================================")
    println(s"Transitive edge reduction started!")
    println(s"Number of edges of the graph before transitive edge reduction: ${dotGraph.edges.count()}")
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

    /**
     * transitive edge reduction algorithms
     * 1. Compute the set of neighbors for each vertex. Retrieve edge property (overlap length) at same time.
     * 2. For each edge, compute the intersection of the sets
     * 3. For each vertex, mark an edge as "False" if it has the intersection, but lower weight.
     * 4. Remove all "False" edges using subgraph. 
     */

    var nbrs = dotGraph.aggregateMessages[Array[(Long, Int, Int)]](
      ctx => {
        // get orietation (0, 1, 2, 3) and overlap length
        var orientation = ctx.attr.split(",")(0).toInt
        val overlapLength = ctx.attr.split(",")(2).toInt

        // send (dstId, orientation, overlapLength) to src
        ctx.sendToSrc(Array((ctx.dstId, orientation, overlapLength)))

        // send (srcId, orientation, overlapLength) to dst
        // orientation should be considered.
        // E.g., 0 (src) >--> 2 (dst) has "3" orientation
        // Then dst "2" will have a neighbor "0" with "3" orientation
        // In the view of node "2", it will have 2 (src) >--> 0 (dst) with "3" orientation.
        // This is wrong and should be updated to "0" orientation
        // "1" and "2" orientation do not need to be considered (same orientation).
        if (orientation == 3) {
          orientation = 0
          ctx.sendToDst(Array((ctx.srcId, orientation, overlapLength)))
        }else if (orientation == 0) {
          orientation = 3
          ctx.sendToDst(Array((ctx.srcId, orientation, overlapLength)))
        }else {
          ctx.sendToDst(Array((ctx.srcId, orientation, overlapLength)))
        }
      },
      (a,b) => a ++ b, TripletFields.All
    )

    // initiate empty array to avoid exception (necessary?)
    nbrs = dotGraph.vertices.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(Long, Int, Int)])
    }   

    // Join the sets with the graph
    val setGraph: Graph[Array[(Long, Int, Int)], String] = dotGraph.outerJoinVertices(nbrs) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }.cache()

    // debug
    if (debug > 0) {
      // print vertices and edges
      println(s"====================================================")
      println(s"setGraph vertices:")
      setGraph.vertices.collect.foreach{
        case (id, arr) => println(s"$id has ${arr.deep}")
      }
      println(s"====================================================")
      println(s"setGraph edges:")
      setGraph.edges.collect.foreach(println(_))
    }

    // Traverse each edge, compute the intersection of the sets, 
    // comprare overlap length of the intersections and orientation
    // Then mark true if the edge can be removed 
    // (Long, Int, Int): (Id, Orientation, Overlap), String: edge properties 
    // => add one more property of Boolean
    val markedGraph: Graph[Array[(Long, Int, Int)], (String, Boolean)] = setGraph.mapTriplets(
      triplet =>  {
        // sort by overlap, from highest to lowest, add - sign
        val srcAttrArray = triplet.srcAttr.sortBy(-_._3)
        val dstAttrArray = triplet.dstAttr.sortBy(-_._3)

        // edgeMark Boolean initialize as false
        var edgeMark: Boolean = false
        
        // get overlap length of the edge
        val overlapLength = triplet.attr.split(",")(2).toInt

        // loop srcAttrArray and get srcMaxOverlapLength:
        breakable {
          for (i <- 0 until srcAttrArray.length) {
            // do not consider vertices of the edge
            val sId = srcAttrArray(i)._1
            if (sId != triplet.srcId && sId != triplet.dstId) {
              // loop dstAttrArray
              for (j <- 0 until dstAttrArray.length) {
                // do not consider vertices of the edge
                val dId = dstAttrArray(j)._1
                if (dId != triplet.srcId && dId != triplet.dstId) {
                  // find the first match and break
                  val type1 = srcAttrArray(i)._2
                  val type2 = dstAttrArray(j)._2
                  val sMaxOverlapLength = srcAttrArray(i)._3
                  val dMaxOverlapLength = dstAttrArray(j)._3
     
                  // condition 1: sid == dId
                  if (sId == dId ) {
                    // condition 2: overlap length should be less than max
                    if (sMaxOverlapLength > overlapLength && dMaxOverlapLength > overlapLength) {
                      // Condition 3: type1 == 1 or 3 => dst In, type2 == 0 or 2 dst Out
                      if ((type1 == 1 || type1 == 3) && (type2 == 0 || type2 == 2 )) {
                        edgeMark = true
                        break
                      }
                      // Condition 3: type1 == 0 or 2 => dst Out, type2 == 1 or 3 dst In
                      else if ((type1 == 0 || type1 == 2) && (type2 == 1 || type2 == 3 )) {
                        edgeMark = true
                        break
                      }
                    }
                  }
                }
              }
            }
          }
        }
        
        if (edgeMark == true) (triplet.attr, true)
          else (triplet.attr, false)

      }
    ).cache()

    // debug
    if (debug > 0) {
      println(s"====================================================")
      println(s"markedGraph was generated:}")
      markedGraph.vertices.collect.foreach(println(_))
      markedGraph.edges.collect.foreach(println(_))
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

    // end module
    println(s"====================================================")
    println(s"Transitive edge reduction done!")
    println(s"Number of edges of the graph after transitive edge reduction: ${remainedGraph.edges.count()}")
    println(s"")

    var edgeStr = remainedGraph.edges.map(e => e.srcId.toString() + "\t" + e.dstId.toString() + "\t" + e.attr._1.toString().replaceAll(":",""))
    edgeStr.saveAsTextFile(resultDirectory)
    println("File saved!")
  }
}

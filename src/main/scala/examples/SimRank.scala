package examples

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

//import simrank.{Vertex}

/**
 * Created by alex on 5/31/16.
 */
object SimRank {

  def main (args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: ")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("sSimRank")

    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "hdfs://node2:9020/eventlog")
    conf.set("spark.storage.memoryFraction", "0.4")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
    /**
    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
    //    conf.set("spark.shuffle.consolidateFiles", "true")

      */
    val sc = new SparkContext(conf)

    /**
    val reverseEdges = sc.textFile(args(0), 120).map(line => {
      val verteices = line.split("\\s+")
      //(Edge(verteices(0).toLong, verteices(1).toLong, verteices(3).toDouble))
      Edge(verteices(1).toLong, verteices(0).toLong, 0)
    })
      */


    /** representation of vertex property:
      * out degree of the vertex;
      * a list of messages(walk) destined to this vertex:
      * {
      *   start vertex of the walk;
      *   length(iteration) of the walk;
      *   multiplicative probability of the walk
      * }
      */

    /**
    val graph = Graph.fromEdges(reverseEdges, (0, Map.empty[Int, List[(Long, Double)]], List.empty[(Long, Int, Double)]))
    simRankPregel(graph, 5)
      */
    val reverseEdges = sc.textFile(args(0), 120).map(line => {
      val verteices = line.split("\\s+")
      (verteices(1).toLong, verteices(0).toLong)
    })
    
  }

  def baselineAllPairSimRank() = {
    null
  }

  def singleSourceSimRank(edgeRDD: RDD[(Long, Long)], iterations: Int, decay: Double, u: Long, v: Long): Unit = {

    def boundaryDemarcation(reverseGraph: RDD[(Long, List[Long])],
                            startVertex: Long,
                            iterations: Int): mutable.HashMap[Long, Int] = {
      val reachables = mutable.HashMap[Long, Int]((startVertex, 0))
      var neighbor = reverseGraph.filter(_._1 == startVertex).map(_._2).collect()(0).toSet
      neighbor.foreach(reachables(_) = 1)

      var level = 2
      do {
        val neighborBD = reverseGraph.sparkContext.broadcast(neighbor)
        val neighborListArray = reverseGraph.filter(e => neighbor.contains(e._1)).map(_._2).collect()

        neighbor = neighborListArray.foldLeft(Set[Long]())((set, list) => set ++ list)
        neighbor.foreach(reachables(_) = level)

        level += 1
      } while (level < iterations)
      reachables -= startVertex
    }

    case class Walk(start: Long, end: Long, length: Int, multiplicity: Double) {}
    case class Walks(walkList: List[Walk]) {}
    //case class Property(inDegree: Int, neighborList: List[Long], passedWalks: Array[]){}

    val graph = edgeRDD.combineByKey[List[Long]](
      (t: Long) => t :: List.empty[Long],
       (list :List[Long], v :Long) => v :: list,
      (listA :List[Long], listB: List[Long]) => listA ::: listB)
    val reverseGraph = edgeRDD.map(t => (t._2, 1)).reduceByKey(_ + _)
    val degreeGraph = reverseGraph.rightOuterJoin(graph).mapValues { case (indegreeOption, neighborList) => {
      indegreeOption match {
        case Some(inDegree) => (inDegree, neighborList)
        case None => (0, neighborList)
      }
    }
    }

    //var walkerRDD = graph.map(e => (e._1, List[(Long, List[(Long, Int)])]((e._1, List.empty[(Long, Int)]))))
    var walkerRDD = graph.map(e => (e._1, List[(Long, List[(Long, Int)])]((e._1, List.empty[(Long, Int)]))))
    var infoRDD = null


    for (iter <- 0 until iterations) {

      walkerRDD = walkerRDD.leftOuterJoin(degreeGraph).flatMap { case (endVertex, (pastWalks, optionProperty)) => {
        optionProperty match {
          case Some((indegree, neighborList)) => {
            val set = mutable.HashSet[Long]()
            for (neighbor <- neighborList)
              yield (neighbor, pastWalks.map(t => (t._1, t._2 :+(neighbor, indegree))))
          }
          case None => {
            List[(Long, List[(Long, List[(Long, Int)])])]((endVertex, pastWalks))
          }
        }
      }
      }.reduceByKey((a, b) => (a ::: b))

      //walkerRDD.count()
      print(iter + "iteration:" + walkerRDD.count())
    }


    /**
    def simRank(edgeRDD: RDD[(Long, Long)], iterations: Int, decay: Double): Unit = {
      val graph = edgeRDD.combineByKey[List[Long]](List[Long](_), (list, v) => v :: list, (listA, listB) => listA ::: listB)
      val reverseGraph = edgeRDD.map(t => (t._2, 1)).reduceByKey(_ + _)

      var walkerRDD = graph.map(e => (e._1, List[(Long, Int, Double)](e._1, 0, 1.0)))
      var infoRDD = reverseGraph.mapValues(inDegree => (inDegree, List.empty[List[(Long, Double)]]))

      var cnt = 0L
      for (iter <- 0 until iterations) {

        walkerRDD = walkerRDD.join(graph).flatMap { case (key, (messages, neighborList)) => {
          for (message <- messages; neighbor <- neighborList)
            yield (neighbor, List[(Long, Int, Double)](message._1, message._2 + 1, message._3))
        }
        }.reduceByKey((a, b) => a ++ b)
        print(iter + "iteration:" + walkerRDD.count())

        /**
        infoRDD = infoRDD.leftOuterJoin(walkerRDD).mapValues { case ((inDegrees, pastWalks), walkList) => {
        if (walkList.isDefined) {
          val walks = walkList.get
          assert(walks.nonEmpty)
          //val key = walks(0)._2
          (inDegrees, pastWalks :+ walks.map(walk => (walk._1, walk._3 * inDegrees)))
        }
        else {
          (inDegrees, pastWalks)
        }
      }
      }
      infoRDD.count()
          */
      }

    }
  }
      */









































  def simRankPregel(graph: Graph[(Int, Map[Int, List[(Long, Double)]], List[(Long, Int, Double)]), Int], iterations: Int): Unit = {
    /** representation of a walk(message):
      * a list of type:
      * {
      *   start vertex of the walk;
      *   length(iteration) of the walk;
      *   multiplicative probability of the walk
      * }
      */
    val degree = graph.outDegrees
    val reverse = graph.outerJoinVertices(degree){ (id, oldAttr, outDeg) =>
    outDeg match {
      case Some(degree) => (degree, oldAttr._2, oldAttr._3)
      case None => oldAttr
    }}
    println("after add degree attribute")
    //reverse.pregel[(Long, Int, Double)](
    val vertices = reverse.pregel[List[(Long, Int, Double)]](
      List.empty[(Long, Int, Double)],
      iterations,
      EdgeDirection.Out
    )(
      (id, values, msg) => {
        if (msg.isEmpty) {
          values
        }
        else if (values._3.isEmpty) {
          (values._1, values._2, msg)
        }
        else {
          (values._1, values._2 + (values._3(0)._2 -> values._3.map(t => (t._1, t._3))), msg)
        }
      },

      triplet => {
        if (triplet.srcAttr._3.isEmpty) {
          Iterator((triplet.dstId, List[(Long, Int, Double)]((triplet.srcId, 0, 1.0))))
        }
        else {
          triplet.srcAttr._3.map(t => (triplet.dstId, List((t._1, t._2 + 1, t._3 * triplet.srcAttr._1)))).toIterator
        }
      },

      (a, b) => (a ++ b)
    ).vertices

    val pair = vertices.flatMap(t => {
      val attr = t._2
      val paths = attr._2 + (attr._3(0)._2 -> attr._3.map(t => (t._1, t._3)))
      val a = ListBuffer.empty[((Long, Long, VertexId), (Int, Double))]
      for ((key, value) <- paths) {
        val m = value.zip(value).map{case((start_1, p_1), (start_2, p_2)) => {
          if (start_1 < start_2) {
            ((start_1, start_2, t._1), (key, p_1 * p_2))
          }
          else {
            ((start_2, start_1, t._1), (key, p_1 * p_2))
          }
        }}
        a ++= m
      }
      a
      })
      .reduceByKey((a, b) => if (a._1 < b._1) a else b)
      .map(t => ((t._1._1, t._1._2), (t._2._2)))
      .reduceByKey((a, b) => a + b).count()
  }
}

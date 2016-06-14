package simrank

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
class DistributedGraph(
                        private var edgesRDD: RDD[Edge],
                        private var nVertices: Long,
                        private var nEdges: Long
                        ) extends Serializable {

  def this(edges: RDD[Edge]) = this(edges, 0L, 0L)

  var verticesRDD: RDD[Vertex] = getVerticesRDD

  def numOfVertices(): Long = {
    if (nVertices <= 0) {
      nVertices = verticesRDD.count()
    }
    nVertices
  }

  private def getVerticesRDD: RDD[Vertex] = edgesRDD.mapPartitions(iter => {
    iter.flatMap(t =>
      Array[Vertex](t.from, t.to)
    )
  }).distinct()

  def numOfEdges(): Long = {
    if (nEdges <= 0) {
      nEdges = edgesRDD.count()
    }
    nEdges
  }
}

class WalkProbability() {
  private var start = 0L
  private var end = 0L
  private var length = 0L
  private var walk =
}

object DistributedGraph{

  def DFS(): Unit = {
  }
}
  */

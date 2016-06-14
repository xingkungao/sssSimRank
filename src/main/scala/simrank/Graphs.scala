package simrank

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.Writable


/**
case class Vertex(id: Long) {}

case class Edge(from: Vertex, to: Vertex, probability: Double) {}


trait Graph extends Serializable with Writable {

  def numOfVertices(): Long

  def numOfEdges(): Long

  def outNeighbours(): List[Vertex]

  //def isSourceVertex(): Boolean

  //def isSinkVertex(v: Vertex): Boolean

  def write(output: DataOutput): Unit

  def readFields(input: DataInput): Unit
}

  */

/**
trait Tree extends Serializable with Writable {

  def numOfVertices(): Long

  def numOfEdges(): Long

  def isRoot(v: Vertex): Boolean

  def isInternalNode(v: Vertex): Boolean

  def isLeafNode(v: Vertex): Boolean

  def getRoot: Vertex

  def getParent(v: Vertex): Vertex

  def getChildren(v: Vertex): List[Vertex]

  def write(output: DataOutput): Unit

  def readFields(input: DataInput): Unit
}

class Graph(
             private var nVertices: Long,
             private var nEdges: Long,
             private var outgoingEdgeList: HashMap[Vertex, HashSet[Vertex]]
             ) extends Serializable with Writable {

  protected var outdated = false

  protected var incomingEdgeList = mutable.HashMap[Vertex, mutable.HashSet[Vertex]]()
  outgoingEdgeList.foreach{ case (vertex, outNeighbor) => {
    outNeighbor.foreach(v => {
      if (incomingEdgeList(v).isEmpty) {
        incomingEdgeList(v) = mutable.HashSet[Vertex]()
      }
      incomingEdgeList(v).add(vertex)
    })
  }}

  def numOfVertices(): Long = {
    if (nVertices <= 0 || outdated) {
      nVertices = getVertices().size
    }
    nVertices
  }

  def numOfEdges(): Long = {
    if (nEdges <= 0 || outdated) {
      nEdges = 0
      outgoingEdgeList.foreach(nEdges += _._2.size)
    }
    nEdges
  }

  def getVertices(): mutable.HashSet[Vertex] = {
    val vertices = mutable.HashSet[Vertex]()
    outgoingEdgeList.foreach(e => {
      vertices.add(e._1)
      e._2.foreach(vertices.add)
    })
    vertices
  }

  def isDeadEnd(u: Vertex) = !outgoingEdgeList.contains(u)

  def write(out: DataOutput): Unit = {
    out.writeLong(numOfVertices())
    out.writeLong(numOfEdges())
    outgoingEdgeList.foreach(e => {
      out.writeLong(e._1.id)
      out.writeLong(e._2.size)
      e._2.foreach(t => out.writeLong(t.id))
    })
  }

  def readFields(in: DataInput): Unit = {
    nVertices = in.readLong()
    nEdges = in.readLong()
    outgoingEdgeList = new HashMap[Vertex, HashSet[Vertex]]()
    for (i <- 0L until nVertices) {
      val key = in.readLong()
      val set = HashSet[Vertex]()
      for (j <- 0L until in.readInt()) {
        set.add(Vertex(in.readLong()))
      }
      outgoingEdgeList(Vertex(key)) = set
    }
  }
}

  */

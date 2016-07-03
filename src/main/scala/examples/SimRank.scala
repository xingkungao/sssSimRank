package examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.mutable


/**
 * Created by alex on 5/31/16.
 */
object SimRank extends Serializable{

  case class Walk(multiplicity: Int, vertices: Array[Long]) {
    def end = vertices.last

    def start = vertices.head

    def length = vertices.length
  }

  def convert(edgeRDD: RDD[(Long, Long)], path: String): Unit = {
    val name = path.split("/").last
    val dir = path.substring(0, path.length - name.length)
    val newPath = dir + "list-" + name

    val graphRDD = edgeRDD.combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).map(t => t._1.toString + " " + t._2.mkString(" "))
    graphRDD.saveAsTextFile(newPath)
  }

  def main (args: Array[String]){
    if (args.length < 4) {
      System.err.println("Usage: ")
      System.exit(-1)
    }
    val startVertex = args(1).toLong
    val iterations = args(2).toInt
    val decay = args(3).toDouble
    val threshold = args(4).toInt

    val conf = new SparkConf().setAppName("sSimRank")

    conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.default.parallelism", "144")
    conf.set("spark.eventLog.dir", "hdfs://node2:9020/eventlog")
    conf.set("spark.storage.memoryFraction", "0.4")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.shuffle.file.buffer.kb", "1024")
    conf.set("spark.akka.threads", "4")
    conf.set("spark.shuffle.consolidateFiles", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
    conf.set("spark.driver.maxResultSize", "40g")
    /**
      * //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      * //    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
      * //    conf.set("spark.shuffle.consolidateFiles", "true")
      * //    conf.set("spark.kryoserializer.buffer.max.mb", "100")
      *
      */

    val sc = new SparkContext(conf)

    val graph = sc.textFile(args(0) + "/*", 120).map(line => {
      val verteices = line.split("\\s+")
      (verteices.head.toLong, verteices.tail.map(_.toLong))
    })

    //val edges = sc.textFile(args(0) + "/*", 120).map(line => {
    //  val verteices = line.split("\\s+")
    //  (verteices(0).toLong, verteices(1).toLong)
    //})//.filter(t => t._1 != t._2).cache()

    //baselineAllPairSimRankLocal(edges, decay, startVertex)
    //baselineAllPairSimRank(edges, startVertex, iterations, decay, threshold)
    //baselineSimRank(edges, startVertex, iterations, decay, threshold)
    //simRankBroadcast(edges, startVertex, iterations, decay, threshold)
    simRank(graph, startVertex, iterations, decay, threshold)
    //convert(edges, args(0))

    /**
      * val m = Array.fill(2, 2)(5)
      * val rdd = sc.parallelize(m, 4).cache()
      * for (a <- rdd.collect()) {
      * a.foreach(println)
      * }
      * println("fuck:")
      * m(0)(1) = 99
      * println("fuck:")
      * for (a <- rdd.collect()) {
      * a.foreach(println)
      * }
      * val nums = Array(99,99)
      * val rdd1 = sc.parallelize(nums, 20)
      * println(rdd1.collect.mkString(" "))
      **
      *nums(0) = 55
      *nums(1) = 55
      *println(rdd1.collect.mkString(" "))
      *val aRDD = sc.parallelize(Array[(Int, Int)]((1,2), (2,4), (2,5)))
      *val bRDD = sc.parallelize(Array[(Int, Int)]((2,8), (2,6)))
      *val c = aRDD.join(bRDD).collect()
      *c.foreach(e => {
      *print(":" + e._1 +  e._2._1.toString() + "    " + e._2._2.toString())
      *})
      *baselineAllPairSimRank()
      */
  }

  def baselineAllPairSimRankLocal(edgeRDD: RDD[(Long, Long)], c:Double, u: Long): Unit = {
    val reverse = edgeRDD.map(t => (t._2, t._1)).combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).collectAsMap()

    val graph = mutable.HashMap[Long, ArrayBuffer[Long]]()
    for ((k, v) <- reverse) {
      for (w <- v) {
        if (!graph.contains(w))
          graph(w) = ArrayBuffer[Long]()
        graph(w) += k
      }
    }

    val vertices = mutable.HashSet[Long]()
    for ((k, v) <- graph) {
      vertices += k
      vertices ++= v
    }

    var cur = mutable.HashMap[(Long, Long), Double]().withDefaultValue(0.0)

    for (v <- vertices; u <- vertices if v >= u) {
      cur((v, u)) = if (v == u) 1.0 else 0.0
    }


    for (k <- 0 until 20) {
      val next = mutable.HashMap[(Long, Long), Double]().withDefaultValue(0.0)
      println("iteration: " + k)
      for (((a,b), v) <- cur) {
        if (a == b) {
          next((a, b)) = 1.0
        }
        else if (reverse.contains(a) && reverse.contains(b)) {
          val mul = c / reverse(a).length / reverse(b).length
          for (w <- reverse(a); t <- reverse(b)) {
            next((a, b)) += mul * cur(if (w > t) (w, t) else (t, w))
          }
        }
        else {
          next((a, b)) = 0.0
        }
      }
      cur = next
    }

    val result = cur.filter(t => t._1._1 == u || t._1._2 == u).map(t =>
      if (t._1._1 == u) (t._1._2, t._2) else (t._1._1, t._2)).toArray.sortBy(_._2)

    result.foreach(println)
  }

  def baselineAllPairSimRank(edgeRDD: RDD[(Long, Long)],
                             startVertex: Long,
                             iterations: Int,
                             decay: Double,
                             threshold: Long): Unit = {
    val reverseGraph1 = edgeRDD.map(t => (t._2, t._1)).combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).cache()

    val reverseGraph = edgeRDD.map(e => (e._2, null))

    /** <vertexID, (inNeighborList, outNeighborList)> */
    val graph = edgeRDD
      .cogroup(reverseGraph)
      .mapValues{case (outNeighbors, inNeighbors) => outNeighbors.toArray}
    println("# of vertices is:" + graph.count())

    /** <(u, v), (outNeighborsU, outNeighborsV), value */
    var squareGraph = graph
      .cartesian(graph)
      .filter{case((u, outNeighborsU), (v, outNeighborsV)) => u >= v}
      .map{case((u, outNeighborsU), (v, outNeighborsV)) =>
        ((u, v), (outNeighborsU, outNeighborsV, if (u == v) 1.0 else 0.0))}
      .cache()

    for (i <- 0 until iterations) {
      val simRDD = squareGraph.flatMap{case ((u, v), (outNeighborsU, outNeighborsV, sim)) =>
        for(a <- outNeighborsU; b <- outNeighborsV)
          yield (if (a >= b) (a, b) else (b, a), (1, sim))
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      squareGraph = squareGraph.leftOuterJoin(simRDD).map{case (key, ((outNeighborsU, outNeighborsV, oldSim), newSimOption)) => {
        if (key._1 == key._2) {
          (key, (outNeighborsU, outNeighborsV, 1.0))
        }
        else {
          newSimOption match {
            case Some((length, sum)) => (key, (outNeighborsU, outNeighborsV, decay / length * sum))
            case None => (key, (outNeighborsU, outNeighborsV, 0.0))
          }
        }
      }}.cache()

      println("in iteration: " + i)
      val result = squareGraph
        .filter(e => e._1._1 == startVertex || e._1._2 == startVertex)
        .map(e => (if (e._1._1 == startVertex) e._1._2 else e._1._1, e._2._3))
        .collect().sortBy(_._2)
      println("#pair:" + result.length)
      result.foreach(println)
      result
    }
  }

  def levelSimRank( graph: mutable.HashMap[Long, (Int, Array[Long])],
                      walks: ArrayBuffer[Walk],
                      root: Long,
                      lastSimRanks: mutable.HashMap[Long, ArrayBuffer[(Long, Int)]],
                      lastLevel: Int,
                      simRanks: mutable.HashMap[Long, ArrayBuffer[(Long, Int)]],
                    threshold: Long): Unit = {
    if (lastSimRanks.isEmpty) {
      for (walk <- walks) {
        val nei = walk.vertices(walk.vertices.length - 2)
        for (neighbor <- graph(root)._2 if neighbor != nei && !simRanks.contains(neighbor)) {
          dfs(graph, simRanks, neighbor, neighbor, 1, walk.length - 1, 1, threshold)
        }
      }
    }
    else {
      for (walk <- walks) {
        val nei = walk.vertices(walk.vertices.length - 2)
        for (neighbor <- graph(root)._2 if neighbor != nei && !simRanks.contains(neighbor)) {
          if (!lastSimRanks.contains(neighbor)) {
            dfs(graph, simRanks, neighbor, neighbor, 1, walk.length - 1, 1, threshold)
          }
          else {
            for ((v, m) <- lastSimRanks(neighbor)) {
              for (neigh <- graph(v)._2) {
                require(walk.length > lastLevel, s"walklength: ${walk.length}, lastlevel: $lastLevel")
                dfs(graph, simRanks, neighbor, neigh, 1, walk.length - lastLevel, m, threshold)
              }
            }
          }
        }
      }
    }
  }


  def dfs(graph: mutable.HashMap[Long, (Int, Array[Long])],
          simRanks: mutable.HashMap[Long, mutable.ArrayBuffer[(Long, Int)]],
          branch: Long,
          vertex: Long,
          level: Int,
          depth: Int,
          multiplicity: Int,
          threshold: Long): Unit = {
    //require(level <= depth, s"level is $level, depth is $depth")
    /**
      * if (!graph.contains(vertex))
      * return
      */

    val newMultiplicity = multiplicity * graph(vertex)._1
    if (newMultiplicity >= threshold)
      return
    if (level == depth) {
      if (!simRanks.contains(branch))
        simRanks(branch) = mutable.ArrayBuffer[(Long, Int)]()
      simRanks(branch) += ((vertex, newMultiplicity))
    }
    else {
      for (neighbor <- graph(vertex)._2) {
        dfs(graph, simRanks, branch, neighbor, level + 1, depth, newMultiplicity, threshold)
      }
    }
  }

  def simRankBroadcast(edgeRDD: RDD[(Long, Long)],
                       startVertex: Long,
                       iterations: Int,
                       decay: Double,
                       threshold: Long): Unit = {
    /**
      * case class Walk(//end: Long,
      * //start: Long,
      * multiplicity: Int,
      * vertices: Array[Long]) {
      * def end = vertices.last
      * def start = vertices.head
      * def length = vertices.length
      * }
      */

    /** calculate local graph */

      /**
    val edges = edgeRDD.collect()
    val localGraph = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]
    for (edge <- edges) {
      if (!localGraph.contains(edge._1))
        localGraph(edge._1) = mutable.ArrayBuffer.empty[Long]
      localGraph(edge._1) += edge._2
    }

    /** calculate local reverse graph */
    val localReverseGraph = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]
    for (edge <- edges) {
      if (!localReverseGraph.contains(edge._2))
        localReverseGraph(edge._2) = mutable.ArrayBuffer.empty[Long]
      localReverseGraph(edge._2) += edge._1
    }
    */
    val localGraph = edgeRDD.combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).collectAsMap()

    val localReverseGraph = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]
    for ((k, v) <- localGraph) {
      for (w <- v) {
        if (!localReverseGraph.contains(w))
          localReverseGraph(w) = mutable.ArrayBuffer.empty[Long]
        localReverseGraph(w) += k
      }
    }

    if (!localReverseGraph.contains(startVertex)) {
      /** terminate */
      return
    }

    /** calculate local indegree graph */
      /**
        * val localIndegreeGraph = localGraph.map(t => (t._1, (
        * if (localReverseGraph.contains(t._1)) localReverseGraph(t._1).size else 0, t._2.toArray)))
        */

    val localIndegreeGraph = mutable.HashMap[Long, (Int, Array[Long])]()
    for ((k, v) <- localReverseGraph) {
      if (localGraph.contains(k)) {
        localIndegreeGraph(k) = (v.length, localGraph(k).toArray)
      }
      else {
        localIndegreeGraph(k) = (v.length, Array.empty[Long])
      }
    }
    for ((k, v) <- localGraph) {
      if (!localIndegreeGraph.contains(k)) {
        localIndegreeGraph(k) = (0, localGraph(k).toArray)
      }
    }

    /** calculate neighborhood */
    val neighborhood = mutable.HashMap[Long, mutable.HashMap[Int, ArrayBuffer[Walk]]]()
    for (i <- 0 until iterations) {
      if (neighborhood.isEmpty) {
        val neighbors = localReverseGraph(startVertex)
        for (neighbor <- neighbors) {
          neighborhood(neighbor) = mutable.HashMap[Int, ArrayBuffer[Walk]](
            (1, ArrayBuffer[Walk](Walk(neighbors.size, Array[Long](startVertex, neighbor)))))
        }
      }
      else {
        val temp = ArrayBuffer[Walk]()
        for ((k, v) <- neighborhood) {
          if (v.contains(i)) {  /** have vertices of last level */
            for (walk <- v(i)) {
              if (localReverseGraph.contains(walk.end)) {
                val neighbors = localReverseGraph(walk.end)
                if (walk.multiplicity * neighbors.length < threshold) {
                  //TODO: filter
                  for (neighbor <- neighbors) {
                    temp += Walk(walk.multiplicity * neighbors.length, walk.vertices :+ neighbor)
                  }
                }
              }
            }
          }
        }
        for (walk <- temp) {
          if (!neighborhood.contains(walk.end)) {
            neighborhood(walk.end) = mutable.HashMap[Int, ArrayBuffer[Walk]]()
          }
          if (!neighborhood(walk.end).contains(i + 1)) {
            neighborhood(walk.end)(i + 1) = ArrayBuffer[Walk]()
          }
          neighborhood(walk.end)(i + 1) += walk
        }
      }
    }
    println("cnt of neighbors:" + neighborhood.size)
    var cnt = 0
    for ((k, v) <- neighborhood)  {
      for ((k1, v1) <- v) {
        cnt += v1.size
      }
    }
    return
    println("cnt of walks:" + cnt)

    for ((v, map) <- neighborhood) {
      for ((length, walks) <- map)
        for (w <- walks) {
          println("walk is:" + w.vertices.mkString("  ") + " ,multiplicity:" + w.multiplicity)
          require(w.length == length + 1, s"w.length: ${w.length}, length: $length")
        }
    }

    val neighborhoodBC = edgeRDD.sparkContext.broadcast(neighborhood)
    val localIndegreeGraphBC = edgeRDD.sparkContext.broadcast(localIndegreeGraph)
    val graph = edgeRDD.map(_._1).distinct()
    val walkRDD = graph.filter(v => neighborhoodBC.value.contains(v)).repartition(144).cache()
    val simRankRDD = walkRDD.flatMap(vertex => {
      val queryWalks = neighborhoodBC.value(vertex)
      val localGraph = localIndegreeGraphBC.value
      var lastSimRanks = mutable.HashMap[Long, mutable.ArrayBuffer[(Long, Int)]]()
      var lastLevel = 0
      val result = mutable.HashMap[Long, Double]().withDefaultValue(0.0)
      for (length <- 1 until (iterations + 2)) {
        if (queryWalks.contains(length)) {
          val walks = queryWalks(length)
          val currentSimRanks = mutable.HashMap[Long, mutable.ArrayBuffer[(Long, Int)]]()
          levelSimRank(localGraph, walks, vertex, lastSimRanks, lastLevel, currentSimRanks, threshold)

          for (w <- walks) {
            val secondLast = w.vertices(w.length - 2)
            for (branch <- localGraph(vertex)._2 if branch != secondLast) {
              if (currentSimRanks.contains(branch)) {
                for (score <- currentSimRanks(branch)) {
                  result(score._1) += math.pow(decay, w.length - 1) / (score._2 * w.multiplicity)
                }
              }
            }
          }

          lastLevel = length + 1
          lastSimRanks = currentSimRanks
        }
      }
      result
    }).cache()

    val t = simRankRDD.reduceByKey(_ + _).collect().sortBy(_._2)
    println("#pair:" + t.length)
    t.foreach(println)
  }


  def boundaryDemarcation(reverseGraph: RDD[(Long, Array[Long])],
                          startVertex: Long,
                           threshold: Long,
                          iterations: Int): mutable.HashMap[Long, mutable.HashMap[Int, ArrayBuffer[Walk]]] = {

    val neighborhood = mutable.HashMap[Long, mutable.HashMap[Int, ArrayBuffer[Walk]]]()
    var lastNeighbors = mutable.HashSet[Long]()
    var currentNeighbors = reverseGraph.filter(_._1 == startVertex).collectAsMap()
    require(currentNeighbors.contains(startVertex))
    for (neighbor <- currentNeighbors(startVertex)) {
      neighborhood(neighbor) = mutable.HashMap[Int, ArrayBuffer[Walk]](
        (1, ArrayBuffer[Walk](Walk(currentNeighbors(startVertex).size, Array[Long](startVertex, neighbor)))))
      lastNeighbors.add(neighbor)
    }

    for (i <- 1 until iterations) {
      val temp = ArrayBuffer[Walk]()
      val lastNeighborBC = reverseGraph.sparkContext.broadcast(lastNeighbors)
      currentNeighbors = reverseGraph.filter(e => lastNeighborBC.value.contains(e._1)).collectAsMap()
      for ((k, v) <- neighborhood) {
        if (v.contains(i)){ /** have vertices of last level */
          for (walk <- v(i)) {
            if (currentNeighbors.contains(walk.end)) {
              val neighbors = currentNeighbors(walk.end)
              if (walk.multiplicity * neighbors.length < threshold) {
                //TODO: filter
                for (neighbor <- neighbors) {
                  temp += Walk(walk.multiplicity * neighbors.length, walk.vertices :+ neighbor)
                }
              }
            }
          }
        }
      }
      lastNeighbors = mutable.HashSet[Long]()
      for (walk <- temp) {
        if (!neighborhood.contains(walk.end)) {
          neighborhood(walk.end) = mutable.HashMap[Int, ArrayBuffer[Walk]]()
        }
        if (!neighborhood(walk.end).contains(i + 1)) {
          neighborhood(walk.end)(i + 1) = ArrayBuffer[Walk]()
        }
        neighborhood(walk.end)(i + 1) += walk
        lastNeighbors.add(walk.end)
      }
    }
    neighborhood
  }

  def simRank(edgeRDD: RDD[(Long, Long)],
              startVertex: Long,
              iterations: Int,
              decay: Double,
              threshold: Long): Unit = {

    /**
      * case class Walk(end: Long,
      * start: Long,
      * lengthLimit: Int,
      * multiplicity: Long,
      * vertices: List[Long]) {
      * def this(end: Long, start: Long, lengthLimit: Int) =
      * this(end, start, lengthLimit, 1L, List[Long](start))
      * }
      */

    val reverseGraph = edgeRDD.map(t => (t._2, t._1)).combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).cache()

    val neighborhood = boundaryDemarcation(reverseGraph, startVertex, threshold, iterations)
    val neighborhoodBC = reverseGraph.sparkContext.broadcast(neighborhood)

    println("cnt of neighbors:" + neighborhood.size)
    var cnt = 0
    for ((k, v) <- neighborhood)  {
      for ((k1, v1) <- v) {
        cnt += v1.size
      }
    }
    println("cnt of walks:" + cnt)
   /**
      **
      *for ((v, map) <- neighborhood) {
      *for ((length, walks) <- map)
      *for (w <- walks) {
      *println("walk is:" + w.vertices.mkString("  ") + " ,multiplicity:" + w.multiplicity)
      *require(w.length == length + 1, s"w.length: ${w.length}, length: $length")
      *}
      *}
      */

    /**
      *val graph = edgeRDD.combineByKey[List[Long]](
      *(t: Long) => t :: List.empty[Long],
      *(list :List[Long], v :Long) => v :: list,
      *(listA :List[Long], listB: List[Long]) => listA ::: listB
      *)
      */

    /** <vertexID, (indegree, adjacencyList)> */
    val graph = reverseGraph
      .map(t => (t._1, t._2.size))
      .cogroup(edgeRDD)
      .mapValues{case (iterableA, iterableB) => (if(iterableA.isEmpty) 0 else iterableA.head, iterableB.toArray)}
      .cache()
    println("# of vertices is:" + graph.count())

    //return

    /** <vertexID, (graphlet, outMostNeighbor)> */
    var propertyGraph = graph
      .filter(v => neighborhoodBC.value.contains(v._1))
      .map(v => (v._1, (mutable.HashMap[Long, (Int, Array[Long])]((v._1, (v._2._1, v._2._2))), mutable.HashSet[Long]() ++= v._2._2)))
      .cache()

    //for (iter <- 0 until iterations) {
    for (iter <- 1 until (iterations + 1)) {
      val outmostRDD = propertyGraph
        .flatMap { case (root, (graphlet, outmostNeighbor)) => outmostNeighbor.map(v => (v, root))}
        .join(graph)
        .map { case (outmost, (root, (indegree, adjacencyList))) =>
          (root, Array[(Long, (Int, Array[Long]))]((outmost, (indegree, adjacencyList))))
        }
        .reduceByKey(_ ++ _)

      propertyGraph = propertyGraph.leftOuterJoin(outmostRDD).mapValues { case ((graphlet, outmostNeighbor), newOutmostInfoOption) => {
        newOutmostInfoOption match {
          case Some(newOutmostInfo) => {
            val newGraphlet = mutable.HashMap[Long, (Int, Array[Long])]()
            newGraphlet ++= newOutmostInfo
            newGraphlet ++= graphlet
            val newOutmostNeighbor = mutable.HashSet[Long]()
            for (neighbors <- newOutmostInfo) {
              newOutmostNeighbor ++= neighbors._2._2
            }
            (newGraphlet, newOutmostNeighbor.filter(!newGraphlet.contains(_)))
          }
          case None => (graphlet, mutable.HashSet[Long]())
        }
      }}
        .cache()
      propertyGraph.count()
    }
    println("now # is" + propertyGraph.count())
    return
    /*
   val tu = propertyGraph.collectAsMap()(331L)
    println("for 331, new outmost is:")
    tu._2.foreach(println)

    println("for 331, graphlet is:")
    val gra = tu._1
    for ((k, v) <- gra) {
      println("k:" + k)
      println(v._2.mkString(" "))
    }
    */
    //return

    val simRankRDD = propertyGraph.flatMap{case (vertex, (graphlet, outmost)) => {
      val queryWalks = neighborhoodBC.value(vertex)
      var lastSimRanks = mutable.HashMap[Long, mutable.ArrayBuffer[(Long, Int)]]()
      var lastLevel = 0
      val result = mutable.HashMap[Long, Double]().withDefaultValue(0.0)
      for (length <- 1 until (iterations + 2)) {
        if (queryWalks.contains(length)) {
          val walks = queryWalks(length)
          val currentSimRanks = mutable.HashMap[Long, mutable.ArrayBuffer[(Long, Int)]]()
          levelSimRank(graphlet, walks, vertex, lastSimRanks, lastLevel, currentSimRanks, threshold)

          for (w <- walks) {
            val secondLast = w.vertices(w.length - 2)
            for (branch <- graphlet(vertex)._2 if branch != secondLast) {
              if (currentSimRanks.contains(branch)) {
                for (score <- currentSimRanks(branch)) {
                  result(score._1) += math.pow(decay, w.length - 1) / (score._2 * w.multiplicity)
                }
              }
            }
          }

          lastLevel = length + 1
          lastSimRanks = currentSimRanks
        }
      }
      result
    }}.cache()

    val t = simRankRDD.reduceByKey(_ + _).collect().sortBy(_._2)
    println("#pair:" + t.length)
    t.foreach(println)
  }


  def baselineSimRank(edgeRDD: RDD[(Long, Long)],
              startVertex: Long,
              iterations: Int,
              decay: Double,
              threshold: Long): Unit = {

    val reverseGraph = edgeRDD.map(t => (t._2, t._1)).combineByKey[Array[Long]](
      (t: Long) => Array[Long](t),
      (list :Array[Long], v :Long) => list :+ v,
      (listA :Array[Long], listB: Array[Long]) => listA ++ listB
    ).cache()

    /** <vertexID, (indegree, adjacencyList)> */
    val graph = reverseGraph
      .map(t => (t._1, t._2.size))
      .cogroup(edgeRDD)
      .mapValues{case (iterableA, iterableB) => (if(iterableA.isEmpty) 0 else iterableA.head, iterableB.toArray)}
      .cache()
    println("# of vertices is:" + graph.count())


    /** <vertexID, (graphlet, outMostNeighbor)> */
    var propertyGraph = graph
      .map(v => (v._1, (mutable.HashMap[Long, (Int, Array[Long])]((v._1, (v._2._1, v._2._2))), mutable.HashSet[Long]() ++= v._2._2)))
      .cache()

    //for (iter <- 0 until iterations) {
    for (iter <- 1 until (iterations)) {
      val outmostRDD = propertyGraph
        .flatMap { case (root, (graphlet, outmostNeighbor)) => outmostNeighbor.map(v => (v, root))}
        .join(graph)
        .map { case (outmost, (root, (indegree, adjacencyList))) =>
          (root, Array[(Long, (Int, Array[Long]))]((outmost, (indegree, adjacencyList))))
        }
        .reduceByKey(_ ++ _).cache()
      outmostRDD.count()

      propertyGraph = propertyGraph.leftOuterJoin(outmostRDD).mapValues { case ((graphlet, outmostNeighbor), newOutmostInfoOption) => {
        newOutmostInfoOption match {
          case Some(newOutmostInfo) => {
            val newGraphlet = mutable.HashMap[Long, (Int, Array[Long])]()
            newGraphlet ++= newOutmostInfo
            newGraphlet ++= graphlet
            val newOutmostNeighbor = mutable.HashSet[Long]()
            for (neighbors <- newOutmostInfo) {
              newOutmostNeighbor ++= neighbors._2._2
            }
            (newGraphlet, newOutmostNeighbor.filter(!newGraphlet.contains(_)))
          }
          case None => (graphlet, mutable.HashSet[Long]())
        }
      }}
        .cache()
      propertyGraph.count()
    }
    println("total is:" + propertyGraph.count())

    def findMasterWalksDFS(graph: mutable.HashMap[Long, (Int, Array[Long])],
                           walksMap: mutable.HashMap[Int, ArrayBuffer[Walk]],
                           target: Long,
                           threshold: Long,
                           level : Int,
                           vertex: Long,
                           walk: Array[Long],
                           mul: Int,
                           depth : Int): Unit = {

      if (!graph.contains(vertex))
        return
      val newMul = mul * graph(vertex)._1
      if (newMul >= threshold)
        return
      if (vertex == target) {
        if (!walksMap.contains(depth))
          walksMap(depth) = mutable.ArrayBuffer[Walk]()
        walksMap(depth) += Walk(newMul, walk :+ vertex)
      }
      if (depth < level) {
        for (w <- graph(vertex)._2)
          findMasterWalksDFS(graph, walksMap, target, threshold, level, w, walk :+ vertex, mul, depth + 1)
      }
    }

    def myMatch(graph: mutable.HashMap[Long, (Int, Array[Long])],
                simRanks: ArrayBuffer[(Long, Int)],
                threshold: Long,
                level: Int,
                vertex: Long,
                mul: Int,
                depth: Int): Unit = {
      if (!graph.contains(vertex))
        return
      val newMul = mul * graph(vertex)._1
      if (newMul >= threshold)
        return
      if (depth == level) {
        simRanks += ((vertex, newMul))
      }
      else {
        for (w <- graph(vertex)._2) {
          myMatch(graph, simRanks, threshold, level, w, newMul, depth + 1)
        }
      }
    }

    val simRankRDD = propertyGraph.flatMap{case (vertex, (graphlet, outmost)) => {
      if (graphlet.contains(vertex)) {
        val result = mutable.HashMap[Long, Double]().withDefaultValue(0.0)
        val masterWalks = mutable.HashMap[Int, ArrayBuffer[Walk]]()

        for (branch <- graphlet(vertex)._2) {
          findMasterWalksDFS(graphlet, masterWalks, startVertex, threshold, iterations, branch, Array[Long](vertex), 1, 1)
        }

        for ((length, walks) <- masterWalks) {
          for (w <- walks) {
            require(w.length == length + 1)
            val simRanks = mutable.ArrayBuffer[(Long, Int)]()
            val subtree = w.vertices(1)
            for (branch <- graphlet(vertex)._2 if branch != subtree) {
              myMatch(graphlet, simRanks, threshold, length, branch, 1, 1)
            }
            for ((node, mul) <- simRanks) {
              result(node) += math.pow(decay, length) / (mul * w.multiplicity)
            }
          }
        }
        result
      }
      else
        null
    }}.cache()

    val t = simRankRDD.reduceByKey(_ + _).collect().sortBy(_._2)
    println("#pair:" + t.length)
    t.foreach(println)
  }
}



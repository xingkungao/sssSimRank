package examples

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.mutable

//import simrank.{Vertex}

/**
 * Created by alex on 5/31/16.
 */
object SimRank {

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
    /**
      * //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      * //    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
      * //    conf.set("spark.shuffle.consolidateFiles", "true")
      * //    conf.set("spark.kryoserializer.buffer.max.mb", "100")
      *
      */

    val sc = new SparkContext(conf)

    val edges = sc.textFile(args(0), 120).map(line => {
      val verteices = line.split("\\s+")
      (verteices(0).toLong, verteices(1).toLong)
    })//.filter(t => t._1 != t._2).cache()

    //simRankBroadcast2(edges, startVertex, iterations, decay, threshold)
    simRank2(edges, startVertex, iterations, decay, threshold)
    baselineAllPairSimRank()
  }

  def baselineAllPairSimRank() = {
    print("fuck your ass")
  }

  def simRankBroadcast(edgeRDD: RDD[(Long, Long)],
                          startVertex: Long,
                          iterations: Int,
                          decay: Double,
                          threshold: Long): Unit = {

    case class Walk(end: Long,
                    start: Long,
                    multiplicity: Long,
                    vertices: List[Long]) {
      def this(start: Long) =
        this(start, start, 1L, List[Long](start))
    }

    val edges = edgeRDD.collect()
    val localGraph = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]
    for (edge <- edges) {
      if (localGraph.contains(edge._1))
        localGraph(edge._1) += edge._2
      else
        localGraph(edge._1) = mutable.ArrayBuffer.empty[Long]
    }

    val localReverseGraph = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]
    for (edge <- edges) {
      if (localReverseGraph.contains(edge._2))
        localReverseGraph(edge._2) += edge._1
      else
        localReverseGraph(edge._2) = mutable.ArrayBuffer.empty[Long]
    }

    val localIndegreeGraph = localGraph.map(t => (t._1, (
      if (localReverseGraph.contains(t._1)) localReverseGraph(t._1).size else 0, t._2.toArray)))

    val distance = mutable.HashMap[Long, mutable.HashSet[Int]]()
    var lastNeighbors = mutable.HashSet[Long](startVertex)
    var currentNeighbors = mutable.HashSet.empty[Long]
    for (i <- 0 until iterations) {
      for (e <- lastNeighbors if localReverseGraph.contains(e)) {
        currentNeighbors ++= localReverseGraph(e)
      }
      currentNeighbors.foreach(t => {
        if (distance.contains(t))
          distance(t) += (i + 1)
        else
          distance(t) = mutable.HashSet.empty[Int]
      })
      lastNeighbors = currentNeighbors
    }

    val distanceBD = edgeRDD.sparkContext.broadcast(distance)

    val localIndegreeGraphBD = edgeRDD.sparkContext.broadcast(localIndegreeGraph)

    /** (id, distanceSet) */
      /**
        * val graph = edgeRDD.combineByKey[List[Long]](
        * (t: Long) => t :: List.empty[Long],
        * (list: List[Long], v: Long) => v :: list,
        * (listA: List[Long], listB: List[Long]) => listA ::: listB
        * )
        * .mapPartitions(iter => {
        * val distanceMap = distanceBD.value
        * iter.map(t => (t._1, (if (distanceMap.contains(t._1)) distanceMap(t._1) else mutable.HashSet.empty[Int], t._2)))
        * })
     */
    val graph = edgeRDD
        .reduceByKey((a, b) => a)
        .mapPartitions(iter => {
          val distanceMap = distanceBD.value
          iter.map(t => (t._1, (if (distanceMap.contains(t._1)) distanceMap(t._1) else mutable.HashSet.empty[Int])))
        })

    val walkRDD = graph.filter(_._2.nonEmpty).flatMap{ case (id, distanceSet) => {
      val localGraph = localIndegreeGraphBD.value
      val limit = distanceSet.max
      // walk limit step from id
      var lastWalks = ListBuffer[Walk](new Walk(id))
      var currentWalks = ListBuffer.empty[Walk]
      val result = ListBuffer.empty[Walk]
      for (i <- 0 until limit) {
        currentWalks = ListBuffer.empty[Walk]
        for (walk <- lastWalks if localGraph.contains(walk.end)) {
          val indegree = localGraph(walk.end)._1
          for (neighbor <- localGraph(walk.end)._2 if indegree * walk.multiplicity <= threshold) {
            currentWalks += Walk(neighbor, walk.start, walk.multiplicity * indegree, walk.vertices :+ neighbor)
          }
        }
        lastWalks = currentWalks
        if (distanceSet.contains(i+1) && currentWalks.nonEmpty) {
          result ++= currentWalks
        }
      }
      result.map(walk => (walk.end, List[Walk](walk)))
    }
    }.reduceByKey(_ ::: _)
        .mapValues(t => {
          val map = mutable.HashMap.empty[Int, List[Walk]]
          t.foreach(walk => {
            val length = walk.vertices.length
            if(map.contains(length))
              map(length)  = map(length) :+ walk
            else
              map(length) = List.empty[Walk]
          })
          map.mapValues(_.toList)
      }).cache()

    val startVertexWalks = walkRDD.filter(_._1 == startVertex).collect().head._2
    val startVertexWalksBD = walkRDD.sparkContext.broadcast(startVertexWalks)
    val simRankRDD = walkRDD.mapPartitions(iter => {
      val toMatchMap = startVertexWalksBD.value
      iter.map(t => {
        val walksMap = t._2
        var score = 0.0
        for (i <- 1 to iterations if (toMatchMap.contains(i) && walksMap.contains(i))) {
          for(a <- toMatchMap(i)) {
            for (b <- walksMap(i)) {
              assert(a.vertices.length == b.vertices.length)
              if (a.start == b.start) {
                var flag = true
                breakable {
                  for (i <- 1 until a.vertices.length) {
                    if (a.vertices(i) == b.vertices(i)) {
                      flag = false
                      break
                    }
                  }
                  if (flag) {
                    score += math.pow(decay, a.vertices.length - 1) * 1.0 / (a.multiplicity * b.multiplicity)
                  }
                }
              }
            }
          }
        }
        (startVertex, t._1, score)
      })
    })
      println(simRankRDD.count())
  }

  def boundaryDemarcation(reverseGraph: RDD[(Long, List[Long])],
                          startVertex: Long,
                          iterations: Int): mutable.HashMap[Long, Int] = {

    val reachables = mutable.HashMap[Long, Int]((startVertex, 0))
    var neighbor = reverseGraph.filter(_._1 == startVertex).map(_._2).collect()(0).toSet
    neighbor.foreach(reachables(_) = 1)

    var level = 2
    do {
      val neighborBD = reverseGraph.sparkContext.broadcast(neighbor)
      val neighborListArray = reverseGraph.filter(e => neighborBD.value.contains(e._1)).map(_._2).collect()

      neighbor = neighborListArray.foldLeft(Set[Long]())((set, list) => set ++ list)
      neighbor.foreach(reachables(_) = level)

      level += 1
    } while (level <= iterations) //TODO: check for equal condition
    reachables -= startVertex
  }

  def SimRank(edgeRDD: RDD[(Long, Long)],
                          startVertex: Long,
                          iterations: Int,
                          decay: Double,
                           threshold: Long): Unit = {

    case class Walk(end: Long,
                    start: Long,
                    lengthLimit: Int,
                    multiplicity: Long,
                    vertices: List[Long]) {
      def this(end: Long, start: Long, lengthLimit: Int) =
        this(end, start, lengthLimit, 1L, List[Long](start))
    }

    val reverseGraph = edgeRDD.map(t => (t._2, t._1)).combineByKey[List[Long]](
        (t: Long) => t :: List.empty[Long],
        (list :List[Long], v :Long) => v :: list,
        (listA :List[Long], listB: List[Long]) => listA ::: listB
      )

    val distance = boundaryDemarcation(reverseGraph, startVertex, iterations)
    println("# of reachable nodes are: " + distance.size)
    val distanceBD = reverseGraph.sparkContext.broadcast(distance)

    val graph = edgeRDD.combineByKey[List[Long]](
        (t: Long) => t :: List.empty[Long],
        (list :List[Long], v :Long) => v :: list,
        (listA :List[Long], listB: List[Long]) => listA ::: listB
      )
      .mapPartitions(iter => {
      val distanceMap = distanceBD.value
      iter.map(t => (t._1, (if (distanceMap.contains(t._1)) distanceMap(t._1) else 0, t._2)))
    }).cache()

    val indegreeGraph = reverseGraph.mapValues(_.length)

    /** <vertexID, (indegree, neighborList, List[List[walk]] */
    var propertyGraph = graph
      .leftOuterJoin(indegreeGraph)
      .mapValues{ case ((distance, neighborList), indegreeOption) => {
      indegreeOption match {
        case Some(indegree) => (indegree, neighborList, Map.empty[Int, List[Walk]])
        case None => (0, neighborList, Map.empty[Int, List[Walk]])
      }
    }}

    //var walkerRDD = graph.map(e => (e._1, List[(Long, List[(Long, Int)])]((e._1, List.empty[(Long, Int)]))))

    for (iter <- 0 until iterations) {

      val walkerRDD =
        if (iter == 0) {
          graph.filter(_._2._1 > 0).flatMap{ case (id, (distance, neighborList)) => {
            for (neighbor <- neighborList)
              yield (neighbor, List[Walk](new Walk(neighbor, id, distance)))
          }}
        }
        else {
          propertyGraph.filter(_._2._3.contains(iter)).flatMap{ case (id, (indegree, neighborList, pastWalks)) => {
            val messages = ListBuffer[(Long, List[Walk])]()
            for (neighbor <- neighborList) {
              val filtered = ListBuffer[Walk]()
              for (walk <- pastWalks(iter) if iter < walk.lengthLimit && walk.multiplicity <= threshold) {
                filtered += walk
              }
              messages += ((neighbor, filtered.toList))
            }
            messages
          }}
        }.reduceByKey(_ ::: _).cache()
      print(iter + "iteration:" + walkerRDD.count())

      propertyGraph = propertyGraph
        .leftOuterJoin(walkerRDD)
        .mapValues{ case ((indegree, neighborList, pastWalks), newWalkListOption) => {
        newWalkListOption match {
          case Some(newWalkList) => {
            (indegree, neighborList, pastWalks + ((iter + 1) -> newWalkList.map(w =>
              Walk(w.end, w.start, w.lengthLimit, w.multiplicity * indegree, w.vertices :+ w.end))))
          }
          case None => (indegree, neighborList, pastWalks)
        }
      }}.cache()
      propertyGraph.count()
    }

    val startVertexWalks = propertyGraph.filter(_._1 == startVertex).collect().head._2._3
    val startVertexWalksBD = propertyGraph.sparkContext.broadcast(startVertexWalks)
    propertyGraph.mapPartitions(iter => {
      val toMatchMap = startVertexWalksBD.value
      iter.map(t => {
        val walksMap = t._2._3
        var score = 0.0
        for (i <- 1 to iterations if (toMatchMap.contains(i) && walksMap.contains(i))) {
          for(a <- toMatchMap(i); b <- walksMap(i)) {
            assert(a.vertices.length == b.vertices.length)
            if(a.start == b.start) {
              var flag = true
              breakable {
                for (i <- 1 until a.vertices.length) {
                  if (a.vertices(i) == b.vertices(i)) {
                    flag = false
                    break
                  }
                }
                if (flag) {
                  score += math.pow(decay, a.vertices.length - 1) * 1.0 / (a.multiplicity * b.multiplicity)
                }
              }
            }
          }
        }
        (startVertex, t._1, score)
      })
    }).count()
  }

  def simRankBroadcast2(edgeRDD: RDD[(Long, Long)],
                       startVertex: Long,
                       iterations: Int,
                       decay: Double,
                       threshold: Long): Unit = {

    def levelSimRank( graph: mutable.HashMap[Long, (Int, Array[Long])],
                      walks: ArrayBuffer[Walk],
                      root: Long,
                      lastSimRanks: mutable.HashMap[Long, ArrayBuffer[(Long, Int)]],
                      lastLevel: Int,
                      simRanks: mutable.HashMap[Long, ArrayBuffer[(Long, Int)]]): Unit = {
      if (lastSimRanks.isEmpty) {
        for (walk <- walks) {
          val nei = walk.vertices(walk.vertices.length - 2)
          for (neighbor <- graph(root)._2 if neighbor != nei && !simRanks.contains(neighbor)) {
            dfs(graph, simRanks, neighbor, neighbor, 1, walk.length - 1, 1)
          }
        }
      }
      else {
        for (walk <- walks) {
          val nei = walk.vertices(walk.vertices.length - 2)
          for (neighbor <- graph(root)._2 if neighbor != nei && !simRanks.contains(neighbor)) {
            if (!lastSimRanks.contains(neighbor)) {
              dfs(graph, simRanks, neighbor, neighbor, 1, walk.length - 1, 1)
            }
            else {
              for ((v, m) <- lastSimRanks(neighbor)) {
                for (neigh <- graph(v)._2) {
                  require(walk.length > lastLevel, s"walklength: ${walk.length}, lastlevel: $lastLevel")
                  dfs(graph, simRanks, neighbor, neigh, 1, walk.length - lastLevel, m)
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
            multiplicity: Int): Unit = {
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
          dfs(graph, simRanks, branch, neighbor, level + 1, depth, newMultiplicity)
        }
      }
    }

    case class Walk(//end: Long,
                    //start: Long,
                    multiplicity: Int,
                    vertices: Array[Long]) {
      def end = vertices.last
      def start = vertices.head
      def length = vertices.length
      /**
        * def this(start: Long, end: Long) =
        * this(1L, Array[Long](start, end))
        * def this(start: Long) =
        * this(start, start, 1L, List[Long](start))
        */
    }

    /** calculate local graph */
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
                for (neighbor <- neighbors) {
                  //TODO: filter
                  if (walk.multiplicity * neighbors.length < threshold)
                    temp += Walk(walk.multiplicity * neighbors.length, walk.vertices :+ neighbor)
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
          levelSimRank(localGraph, walks, vertex, lastSimRanks, lastLevel, currentSimRanks)

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

  case class Walk(multiplicity: Int, vertices: Array[Long]) {
    def end = vertices.last

    def start = vertices.head

    def length = vertices.length
  }
  def boundaryDemarcation2(reverseGraph: RDD[(Long, List[Long])],
                          startVertex: Long,
                           threshold: Int,
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
        if ((v.contains(i))){ /** have vertices of last level */
          for (walk <- v(i)) {
            if (currentNeighbors.contains(walk.end)) {
              val neighbors = currentNeighbors(walk.end)
              for (neighbor <- neighbors) {
                //TODO: filter
                if (walk.multiplicity * neighbors.length < threshold)
                  temp += Walk(walk.multiplicity * neighbors.length, walk.vertices :+ neighbor)
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

  def simRank2(edgeRDD: RDD[(Long, Long)],
              startVertex: Long,
              iterations: Int,
              decay: Double,
              threshold: Long): Unit = {

    case class Walk(end: Long,
                    start: Long,
                    lengthLimit: Int,
                    multiplicity: Long,
                    vertices: List[Long]) {
      def this(end: Long, start: Long, lengthLimit: Int) =
        this(end, start, lengthLimit, 1L, List[Long](start))
    }

    val reverseGraph = edgeRDD.map(t => (t._2, t._1)).combineByKey[List[Long]](
      (t: Long) => t :: List.empty[Long],
      (list :List[Long], v :Long) => v :: list,
      (listA :List[Long], listB: List[Long]) => listA ::: listB
    )

    val distance = boundaryDemarcation(reverseGraph, startVertex, iterations)
    println("# of reachable nodes are: " + distance.size)
    val distanceBD = reverseGraph.sparkContext.broadcast(distance)

    val graph = edgeRDD.combineByKey[List[Long]](
      (t: Long) => t :: List.empty[Long],
      (list :List[Long], v :Long) => v :: list,
      (listA :List[Long], listB: List[Long]) => listA ::: listB
    ).mapPartitions(iter => {
        val distanceMap = distanceBD.value
        iter.map(t => (t._1, (if (distanceMap.contains(t._1)) distanceMap(t._1) else 0, t._2)))
      }).cache()

    val indegreeGraph = reverseGraph.mapValues(_.length)

    /** <vertexID, (indegree, neighborList, List[List[walk]])> */
    var propertyGraph = graph
      .leftOuterJoin(indegreeGraph)

      .mapValues{ case ((distance, neighborList), indegreeOption) => {
        indegreeOption match {
          case Some(indegree) => (indegree, neighborList, Map.empty[Int, List[Walk]])
          case None => (0, neighborList, Map.empty[Int, List[Walk]])
        }
      }}

    //var walkerRDD = graph.map(e => (e._1, List[(Long, List[(Long, Int)])]((e._1, List.empty[(Long, Int)]))))

    for (iter <- 0 until iterations) {

      val walkerRDD =
        if (iter == 0) {
          graph.filter(_._2._1 > 0).flatMap{ case (id, (distance, neighborList)) => {
            for (neighbor <- neighborList)
              yield (neighbor, List[Walk](new Walk(neighbor, id, distance)))
          }}
        }
        else {
          propertyGraph.filter(_._2._3.contains(iter)).flatMap{ case (id, (indegree, neighborList, pastWalks)) => {
            val messages = ListBuffer[(Long, List[Walk])]()
            for (neighbor <- neighborList) {
              val filtered = ListBuffer[Walk]()
              for (walk <- pastWalks(iter) if iter < walk.lengthLimit && walk.multiplicity <= threshold) {
                filtered += walk
              }
              messages += ((neighbor, filtered.toList))
            }
            messages
          }}
        }.reduceByKey(_ ::: _).cache()
      print(iter + "iteration:" + walkerRDD.count())

      propertyGraph = propertyGraph
        .leftOuterJoin(walkerRDD)
        .mapValues{ case ((indegree, neighborList, pastWalks), newWalkListOption) => {
          newWalkListOption match {
            case Some(newWalkList) => {
              (indegree, neighborList, pastWalks + ((iter + 1) -> newWalkList.map(w =>
                Walk(w.end, w.start, w.lengthLimit, w.multiplicity * indegree, w.vertices :+ w.end))))
            }
            case None => (indegree, neighborList, pastWalks)
          }
        }}.cache()
      propertyGraph.count()
    }

    val startVertexWalks = propertyGraph.filter(_._1 == startVertex).collect().head._2._3
    val startVertexWalksBD = propertyGraph.sparkContext.broadcast(startVertexWalks)
    propertyGraph.mapPartitions(iter => {
      val toMatchMap = startVertexWalksBD.value
      iter.map(t => {
        val walksMap = t._2._3
        var score = 0.0
        for (i <- 1 to iterations if (toMatchMap.contains(i) && walksMap.contains(i))) {
          for(a <- toMatchMap(i); b <- walksMap(i)) {
            assert(a.vertices.length == b.vertices.length)
            if(a.start == b.start) {
              var flag = true
              breakable {
                for (i <- 1 until a.vertices.length) {
                  if (a.vertices(i) == b.vertices(i)) {
                    flag = false
                    break
                  }
                }
                if (flag) {
                  score += math.pow(decay, a.vertices.length - 1) * 1.0 / (a.multiplicity * b.multiplicity)
                }
              }
            }
          }
        }
        (startVertex, t._1, score)
      })
    }).count()
  }
}



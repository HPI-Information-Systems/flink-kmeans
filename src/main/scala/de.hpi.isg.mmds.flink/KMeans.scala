package de.hpi.isg.mmds.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.util.Random

// Important! Implicits...
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._

/**
  * This is a simple Spark implementation of the k-means algorithm.
  *
  * @param inputUrl URL to a input CSV file
  * @param k        number of clusters to be created
  */
class KMeans(inputUrl: String, k: Int, numIterations: Int) {

  /**
    * Execution environment for Apache Flink.
    */
  val env = ExecutionEnvironment.getExecutionEnvironment

  def run() = {
    // Read and parse the points from an input file.
    val pointsDataSet = env.readCsvFile[Point](inputUrl, fieldDelimiter = ",", pojoFields = Array("x", "y"))

    // Generate random initial centroids.
    val initialCentroidsDataSet = env.fromCollection(KMeans.createRandomCentroids(k))

    // Iterate. In each iteration, we change the centroids, so that's where we invoke the iterate method on.
    val finalCentroidsDataSet = initialCentroidsDataSet.iterate(numIterations) { centroidsDataSet =>

      // Select the nearest centroids for each point.
      val nearestCentroidDataSet = pointsDataSet
        .map(new SelectNearestCentroid())
        .withBroadcastSet(centroidsDataSet, KMeans.centroidBc)

      // Calculate the new centroids.
      val _k = k
      val newCentroids = nearestCentroidDataSet
        .groupBy("centroidId")
        .sum("count").andSum("x").andSum("y")
        .map(_.average).withForwardedFields("centroidId")
        .reduceGroup { (iterator: Iterator[TaggedPoint], out: Collector[TaggedPoint]) =>
          val centroids = iterator.toArray
          centroids.foreach(out.collect)
          KMeans.createRandomCentroids(_k - centroids.length).foreach(out.collect)
        }

      // Return the new centroids to be used in the next iteration.
      newCentroids
    }

    // Only here we trigger the execution.
    val centroids = finalCentroidsDataSet.collect()

    // Print the result.
    println("Results:")
    centroids.foreach(println _)
  }

}

/**
  * Companion object for [[KMeans]].
  */
object KMeans {

  /** Key for centroid broadcasts. */
  val centroidBc = "centroids"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input URL> <k> <#iterations>")
      sys.exit(1)
    }

    val startTime = java.lang.System.currentTimeMillis
    new KMeans(args(0), args(1).toInt, args(2).toInt).run()
    val endTime = java.lang.System.currentTimeMillis

    println(f"Finished in ${endTime - startTime}%,d ms.")
  }

  /**
    * Creates random centroids.
    *
    * @param n      the number of centroids to create
    * @param random used to draw random coordinates
    * @return the centroids
    */
  def createRandomCentroids(n: Int, random: Random = new Random()) =
    for (i <- 1 to n) yield TaggedPoint(random.nextDouble(), random.nextDouble(), i)

}

/**
  * UDF to select the closest centroid for a given [[Point]].
  */
@ForwardedFields(Array("x->x", "y->y"))
class SelectNearestCentroid extends RichMapFunction[Point, TaggedPointCounter] {

  /** Keeps the broadcasted centroids. */
  var centroids: Seq[TaggedPoint] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    centroids = getRuntimeContext.getBroadcastVariable[TaggedPoint](KMeans.centroidBc).asScala
  }

  override def map(point: Point): TaggedPointCounter = {
    var minDistance = Double.PositiveInfinity
    var nearestCentroidId = -1
    for (centroid <- centroids) {
      val distance = point.distanceTo(centroid)
      if (distance < minDistance) {
        minDistance = distance
        nearestCentroidId = centroid.centroidId
      }
    }
    new TaggedPointCounter(point, nearestCentroidId, 1)
  }
}

/**
  * Represents objects with an x and a y coordinate.
  */
sealed trait PointLike {

  /**
    * @return the x coordinate
    */
  def x: Double

  /**
    * @return the y coordinate
    */
  def y: Double

}

/**
  * Represents a two-dimensional point.
  *
  * @param x the x coordinate
  * @param y the y coordinate
  */
case class Point(x: Double, y: Double) extends PointLike {

  /**
    * Calculates the Euclidean distance to another [[Point]].
    *
    * @param that the other [[PointLike]]
    * @return the Euclidean distance
    */
  def distanceTo(that: PointLike) = {
    val dx = this.x - that.x
    val dy = this.y - that.y
    math.sqrt(dx * dx + dy * dy)
  }

}

/**
  * Represents a two-dimensional point with a centroid ID attached.
  */
case class TaggedPoint(x: Double, y: Double, centroidId: Int) extends PointLike

/**
  * Represents a two-dimensional point with a centroid ID and a counter attached.
  */
case class TaggedPointCounter(x: Double, y: Double, centroidId: Int, count: Int) extends PointLike {

  def this(point: PointLike, centroidId: Int, count: Int) = this(point.x, point.y, centroidId, count)

  /**
    * Adds coordinates and counts of two instances.
    *
    * @param that the other instance
    * @return the sum
    */
  def +(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.centroidId, this.count + that.count)

  /**
    * Calculates the average of all added instances.
    *
    * @return a [[TaggedPoint]] reflecting the average
    */
  def average = TaggedPoint(x / count, y / count, centroidId)

}

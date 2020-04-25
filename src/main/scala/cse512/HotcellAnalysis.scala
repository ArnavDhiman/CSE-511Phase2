package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  ////
  // CHANGED PART
  ///
  // TODO: See if $ sign works in string
  val nycPickup = spark.sql("SELECT x,y,z FROM tempView where x >=$minX and y >= $minY and z >= $minZ and x <= $maxX and y <= $maxY and z <= $maxZ");
  nycPickup.createOrReplaceTempView("temp");

  val X = spark.sql("SELECT x,y,z, COUNT(*) as xCount FROM temp GROUP BY x,y,z");
  X.createOrReplaceTempView("tempX")
  X.show()

  val neighbor = spark.sql("SELECT t1.x, t1.y, t1.z, SUM(t2.xCount) as sumX, COUNT(t2.xCount) as neighbors " +
    "FROM tempX t1, tempX t2 " +
    "WHERE ((ABS(t1.x-t2.x) = 1 OR ABS(t1.x-t2.x) = 0) AND (ABS(t1.y-t2.y) = 1 OR ABS(t1.y-t2.y) = 0) AND (ABS(t1.z-t2.z) = 1 OR ABS(t1.z-t2.z) = 0)) " +
    "GROUP BY t1.x, t1.y, t1.z");
  neighbor.createOrReplaceTempView("tempNeighbor")
  neighbor.show()

  val sum1 = spark.sql("SELECT SUM(tempX.xCount) as sumX FROM tempX").first().getLong(0).toDouble
  val sum2 = spark.sql("SELECT SUM(tempX.xCount * tempX.xCount) as sumX2 FROM tempX").first().getLong(0).toDouble

  var mean = sum1 / numCells.toDouble
  mean = mean.toDouble

  var stdDev = math.sqrt(sum2/numCells - mean* mean)
  stdDev = stdDev.toDouble

  spark.udf.register("zScore", (x: Int, y: Int, z: Int, sum: Double, neighbors: Int) =>
    HotcellUtils.score(x, y, z, minX, maxX, minY, maxY, minZ, maxZ, numCells.toDouble, sum, neighbors, mean, stdDev)
    )

  val result = spark.sql("SELECT _.x, _.y, _.z FROM (SELECT x, y, z, zScore(tempNeighbor.x, tempNeighbor.y, tempNeighbor.z, tempNeighbor.sumNX, tempNeighbor.neighbors) as score FROM tempNeighbor ORDER BY score DESC) _")
  result.show()

  return result

}
}

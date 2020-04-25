package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    ////
    // ST_CONTAINS from phase 1
    ////
    val points = pointString.split(",")
    val rectangle = queryRectangle.split(",")
    val px = points(0).toDouble
    val py = points(1).toDouble
    val rx1 = rectangle(0).toDouble
    val ry1 = rectangle(1).toDouble
    val rx2 = rectangle(2).toDouble
    val ry2 = rectangle(3).toDouble

    if (px >= rx1 && px <= rx2 && py >= ry1 && py <= ry2) true
    else false
  }
}

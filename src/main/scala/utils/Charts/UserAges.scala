package utils.Charts

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class UserAges {
  def showChart(userDf: DataFrame, min: Int = 0, max: Int = 80, bins: Int = 16) = {
    val ages = userDf.select("age").collect()

    val step: Int = max/bins
    val mx = new mutable.MapBuilder[Int, Int]()
    for (i <- 0 until (max + step) by step) {
      mx += (i -> 0)
    }
    for (x <- ages.indices) {
      val age = Integer.parseInt(ages(x)(0).toString)
      for (j <- 0 until (max + step) by step) {
        if (age >= j && age < (j + step)) {
          mx += (j -> (mx(j) + 1))
        }
      }
    }
  }
}

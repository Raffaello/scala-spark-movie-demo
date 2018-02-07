package utils.Charts

import org.apache.spark.sql.DataFrame
import org.jfree.data.category.DefaultCategoryDataset

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories

object UserAges {

  private def buildChart(userDf: DataFrame, min: Int, max: Int, bins: Int) = {
    val ages = userDf.select("age").collect()
    val step: Int = max/bins
    var mapXs = Map[Int, Int](0 -> 0)

    for (i <- step until (max + step) by step) {
      mapXs += (i -> 0)
    }

    for (x <- ages.indices) {
      val age = Integer.parseInt(ages(x)(0).toString)
      for (j <- 0 until (max + step) by step) {
        if (age >= j && age < (j + step)) {
          mapXs = mapXs + (j -> (mapXs(j) + 1))
        }
      }
    }

    val mxSorted = ListMap(mapXs.toSeq.sortBy(_._1):_*)
    val ds = new DefaultCategoryDataset()
    mxSorted.foreach{ case (k, v) => ds.addValue(v, "UserAges", k)}

    ChartFactories.BarChart(ds)
  }

  def showChart(userDf: DataFrame, min: Int = 0, max: Int = 80, bins: Int = 16): Unit = {
    buildChart(userDf, min, max, bins).show()
  }
}

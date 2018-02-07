package utils.Charts

import java.awt.Font

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.data.category.DefaultCategoryDataset

import scalax.chart.module.ChartFactories

object MovieAges {

  def movieYearsCountSorted(movieDf: DataFrame)(implicit spark: SparkSession): Array[(Int, String)] = {
    movieDf.createOrReplaceTempView("movie_data")

    spark.udf.register("convertYear", (x: String) => try {
      x.takeRight(4).toInt
    } catch {
      case e: Exception => {
        println("exception caught: " + e + " Returning 1900")
        1900
      }
    })

    spark.sql("SELECT convertYear(date) AS year FROM movie_data")
      .groupBy("year")
      .count()
      .rdd
      .map(row => (Integer.parseInt(row(0).toString), row(1).toString))
      .collect()
      .sortBy(_._1)
  }

  def showChart(movieDf: DataFrame)(implicit spark: SparkSession) = {
    val ds = new DefaultCategoryDataset
    for (i <- movieYearsCountSorted(movieDf)) {
      ds.addValue(i._2.toDouble, "year", i._1)
    }

    val chart = ChartFactories.BarChart(ds)
    chart.peer.getCategoryPlot.getDomainAxis
      .setCategoryLabelPositions(CategoryLabelPositions.UP_90)
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(new Font("Dialog", Font.PLAIN, 5))

    chart.show()
  }
}

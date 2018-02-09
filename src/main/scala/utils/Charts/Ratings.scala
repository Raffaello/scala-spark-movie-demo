package utils.Charts

import java.awt.Font

import org.apache.spark.sql.DataFrame
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.data.category.DefaultCategoryDataset

import scala.collection.immutable.ListMap
import scalax.chart.module.CategoryChartFactories.BarChart

object Ratings {

  def showUserRatings(ratingDf: DataFrame) = {

    val ratingDfCountCollection = ratingDf.groupBy("rating").count().sort("rating").collect()
    val ds = new DefaultCategoryDataset()

    for (x <- ratingDfCountCollection.indices) {
      val occ = ratingDfCountCollection(x)(0)
      val count = Integer.parseInt(ratingDfCountCollection(x)(1).toString)
      ds.addValue(count, "UserRatings", occ.toString)
    }

    val chart = BarChart(ds)
    chart.peer.getCategoryPlot.getDomainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_90)
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(new Font("Dialog", Font.PLAIN, 5))
    chart.show()
  }

  def showRatings(ratingDf: DataFrame, max: Int = 1000, bins: Int = 100) = {

    val ds = new DefaultCategoryDataset
    var mx = Map(0 -> 0)
    val step = max / bins

    for(i <- step until (max + step) by step) {
      mx += (i -> 0)
    }
    val ratingNoByUserCollect = ratingDf.groupBy("user_id").count().sort("count").collect()
    for( x <- ratingNoByUserCollect.indices) {
      val user_id = Integer.parseInt(ratingNoByUserCollect(x)(0).toString)
      val count = Integer.parseInt(ratingNoByUserCollect(x)(1).toString)
      ds.addValue(count,"Ratings", user_id)
    }

    val chart = BarChart(ds)
    chart.peer.getCategoryPlot.getDomainAxis().setVisible(false)

    chart.show()
  }
}

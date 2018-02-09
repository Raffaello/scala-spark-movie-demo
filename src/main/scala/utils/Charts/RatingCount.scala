package utils.Charts

import java.awt.Font

import org.apache.spark.sql.DataFrame
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.data.category.DefaultCategoryDataset

import scalax.chart.module.CategoryChartFactories.BarChart

object RatingCount {

  def showChart(ratingDf: DataFrame) = {

    val ratingDfCountCollection = ratingDf.groupBy("rating").count().sort("rating").collect()
    val ds = new DefaultCategoryDataset()

    for (x <- ratingDfCountCollection.indices) {
      val occ = ratingDfCountCollection(x)(0)
      val count = Integer.parseInt(ratingDfCountCollection(x)(1).toString)
      ds.addValue(count, "UserRating", occ.toString)
    }

    val chart = BarChart(ds)
    chart.peer.getCategoryPlot.getDomainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_90)
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(new Font("Dialog", Font.PLAIN, 5))
    chart.show()
  }
}

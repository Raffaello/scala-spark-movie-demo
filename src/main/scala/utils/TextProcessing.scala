package utils

import breeze.linalg.CSCMatrix
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object TextProcessing {

  private def dropTitleYear(title: String): String = "^[^(]*".r.findFirstIn(title).getOrElse("")

  private def createVector(titleTerms: Array[String], allTermsDict: Map[String, Int]): CSCMatrix[Int] = {
    val A = CSCMatrix.zeros[Int](1, allTermsDict.size)
    titleTerms.foreach(term => {
      if (allTermsDict.contains(term)) {
        A.update(0, allTermsDict(term), 1)
      }
    })

    A
  }

  def processTitles(movieDf: DataFrame)(implicit spark: SparkSession) = {
    movieDf.select("name").createOrReplaceTempView("titles")
    spark.udf.register("dropTitleYear", dropTitleYear _)
    val processedTitles = spark.sql("SELECT dropTitleYear(name) FROM titles")
    val titlesRDD = processedTitles.rdd.map(x => x(0).toString).map(x => x.split(" "))

    titlesRDD.take(5).foreach(_.foreach(println))
    val allTermsDict = new ListBuffer[String]()
//    val allTermsWithZip = titlesRDD.flatMap(_).distinct().zipWithIndex().collectAsMap()
    val allTerms = titlesRDD.flatMap(x => x).distinct().collect()
    for(t <- allTerms) {
      allTermsDict  += t
    }

    allTermsDict.zipWithIndex.toMap
    val termVectors = titlesRDD.map(t => createVector(t, allTermsDict.zipWithIndex.toMap))
//    termVectors.take(5).foreach(println)
  }
}

name := "scala-spark-movie-demo"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "latest.integration"
libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"


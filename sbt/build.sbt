name := "sparkCassandra2"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7"
)
    

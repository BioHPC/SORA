name := "Composite Edge Contraction Project"

version := "1.1"

scalaVersion := "2.11.8"


resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-graphx" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0",
    "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11" 

)
assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

name := "MongoDB_Redshift_Export"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers ++= Seq(
  "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/"
)

val awsSDKVersion = "1.10.22"

libraryDependencies ++= Seq(
  "org.apache.spark"        %%  "spark-core"      % "1.6.0" % "provided",
  "org.apache.spark"        %%  "spark-sql"       % "1.6.0" % "provided",
  "org.mongodb.mongo-hadoop" %  "mongo-hadoop-core" % "1.5.2",
  ("com.databricks" %% "spark-redshift" % "1.1.0").
            exclude("org.apache.avro", "avro"),
  ("org.apache.hadoop" % "hadoop-aws" % "2.7.4"),
  "net.java.dev.jets3t" % "jets3t" % "0.9.4"
)

mergeStrategy in assembly := {
   case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
   case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
   case "log4j.properties" => MergeStrategy.discard
   case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
   case "reference.conf" => MergeStrategy.concat
   case _ => MergeStrategy.first
}



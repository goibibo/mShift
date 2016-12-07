name := "MongoDB_Redshift_Export"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions += "-deprecation"
scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "org.apache.spark"        %%  "spark-core"      % "1.6.0" % "provided",
  "org.apache.spark"        %%  "spark-sql"       % "1.6.0" % "provided",
  "com.databricks"          %%  "spark-redshift"  % "1.1.0" ,
  "org.mongodb.mongo-hadoop" %  "mongo-hadoop-core" % "1.5.2",
  "com.amazonaws"           %   "aws-java-sdk"    % "1.7.4",
  "org.apache.hadoop"       %   "hadoop-aws"      % "2.7.2"
)

mergeStrategy in assembly := {
   case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
   case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
   case "log4j.properties" => MergeStrategy.discard
   case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
   case "reference.conf" => MergeStrategy.concat
   case _ => MergeStrategy.first
}



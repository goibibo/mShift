name := "MongoDB_Redshift_Export"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

resolvers += "Cloudera Repo for Spark-HBase" at "https://repository.cloudera.com/content/repositories/releases/"
resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions += "-deprecation"
scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "org.apache.spark"        %%  "spark-core"      % "1.6.0" % "provided",
  "org.apache.spark"        %%  "spark-sql"       % "1.6.0" % "provided",
  "org.apache.spark"        %% "spark-streaming" % "1.6.0" % "provided",
  "com.goibibo" %% "dataplatform_utils" % "1.6",
  ("org.apache.spark" %% "spark-streaming-kafka" % "1.6.0-cdh5.8.2").
    exclude("org.apache.kafka", "kafka_2.10").
    exclude("org.spark-project.spark", "unused"),
  ("org.apache.kafka" %% "kafka" % "0.9.0.0").
            exclude("org.slf4j", "slf4j-api").
            exclude("net.jpountz.lz4", "lz4").
            exclude("org.apache.zookeeper", "zookeeper").
            exclude("org.slf4j", "slf4j-log4j12").
            exclude("log4j", "log4j"),
  "org.mongodb.mongo-hadoop" %  "mongo-hadoop-core" % "1.5.2" excludeAll(
     ExclusionRule(organization = "org.apache.hadoop")),
  ("org.mongodb" % "mongo-java-driver" % "3.2.1") excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "org.slf4j")),
  ("com.databricks"          %%  "spark-redshift"       % "1.1.0").
    exclude("org.apache.avro","avro").
    exclude("org.slf4j","slf4j-api"),
  "com.jsuereth"            %% "scala-arm"              % "2.0",
  ("com.amazonaws"           %   "aws-java-sdk-core"    % "1.10.22").
    exclude("com.fasterxml.jackson.core","jackson-core").
    exclude("com.fasterxml.jackson.core","jackson-databind").
    exclude("com.fasterxml.jackson.core","jackson-annotations").
    exclude("commons-codec", "commons-codec").
    exclude("commons-logging", "commons-logging").
    exclude("joda-time","joda-time").
    exclude("org.apache.httpcomponents","httpclient"),
  ("com.amazonaws" % "aws-java-sdk-s3" % "1.10.22").
    exclude("com.amazonaws","aws-java-sdk-core"),
  ("org.apache.hadoop"       % "hadoop-aws"            % "2.6.0-cdh5.8.2").
    exclude("com.amazonaws","aws-java-sdk-s3").
    exclude("com.fasterxml.jackson.core","jackson-databind").
    exclude("com.fasterxml.jackson.core","jackson-annotations")
    exclude("org.apache.hadoop","hadoop-common")
)

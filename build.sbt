name := "spark-dynamodb"

version := "0.1"

organization := "com.databricks"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.9.17"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/cfregly/spark-dynamodb</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:cfregly/spark-dynamodb.git</url>
    <connection>scm:git:git@github.com:cfregly/spark-dynamodb.git</connection>
  </scm>
  <developers>
    <developer>
      <id>cfregly</id>
      <name>Chris Fregly</name>
      <url>https://github.com/cfregly</url>
    </developer>
  </developers>)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

name := "learnSparkBostonCrimes"
version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.2.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1" % Provided

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := name.value + "-" + version.value + ".jar",
  assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.discard
  case x => (assembly / assemblyMergeStrategy).value.apply(x)
  }
)

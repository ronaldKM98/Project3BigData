name := "Project3BigData"
organization := "edu.eafit"
version := "1.0.0"
scalaVersion := "2.12.8"
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "utf-8",
  "-explaintypes",
  "-feature",
  "-unchecked",
  "-Xlint:infer-any",
  "-Xlint:unsound-match",
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any"
)
scalacOptions += "-target:jvm-1.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")


//initialize := {
  //val _ = initialize.value
  //if (sys.props("java.specification.version") != "1.8")
 //   sys.error("Java 8 is required for this project.")
//}

val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
)

// Execute run in a separate JVM.
fork in run := true

// Hack for getting the Provided dependencies work in the run task.
fullClasspath in Runtime := (fullClasspath in Compile).value
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
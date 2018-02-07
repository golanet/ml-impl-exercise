scalaVersion := "2.11.11"

lazy val root =
  Project("exercise", file(".")) // Set your project name here; this identifier is only used within SBT
    .settings(
      Seq(
        name := "exercise",
        version := "1.0",
        mainClass in Compile := Some("exercise.Main"),
        fork in run := true
      )
    )

val sparkVersion = "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion //% "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

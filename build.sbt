
name := "Csv Lib"
organization := "org.ronruby"
organizationHomepage := None
version := "0.1"

lazy val root = project in file(".")

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
scalacOptions in Compile ++= Seq("-deprecation", "-explaintypes", "-feature", "-unchecked")
Compile / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n == 12 => Seq("-language:higherKinds", "-Ypartial-unification")
    case _ => Seq.empty
  }
}
crossScalaVersions := Seq("2.13.3")
scalaVersion := crossScalaVersions.value.head
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oSD")

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-guice" % "2.8.2",
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-kernel-laws" % "2.1.1" % "test",
  "org.typelevel" %% "cats-laws" % "2.1.1" % "test",
  "org.typelevel" %% "discipline-core" % "1.0.2" % "test",
  "dev.zio" %% "zio-interop-cats" % "2.1.4.0",
  "dev.zio" %% "zio" % "1.0.0",
  "au.com.bytecode" % "opencsv" % "2.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
  "org.mockito" %% "mockito-scala-scalatest" % "1.14.8" % "test",
  "org.mockito" % "mockito-inline" % "3.3.3" % "test",
  "org.scalatestplus.play" %% "scalatestplus-play" % "5.1.0" % "test",
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % "test"
)

scapegoatConsoleOutput := true
scapegoatIgnoredFiles := Seq.empty
scapegoatVersion in ThisBuild := "1.4.5"

coverageFailOnMinimum := true
coverageHighlighting := true
coverageMinimum := 100
coverageExcludedPackages := """<empty>;..*Module.*;"""


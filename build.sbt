name := "LightDB"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.deephacks.lmdbjni" % "lmdbjni-linux64" % "0.4.4",
  "org.deephacks.lmdbjni" % "lmdbjni" % "0.4.4",
  "org.deephacks.lmdbjni" % "lmdbjni-osx64" % "0.4.4",
  // "org.deephacks.lmdbjni" % "lmdbjni-win64" % "0.4.2",
  "com.googlecode.json-simple" % "json-simple" % "1.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "junit" % "junit" % "4.10" % "test")
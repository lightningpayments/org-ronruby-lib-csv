[![Build Status](https://travis-ci.org/zamblauskas/scala-csv-parser.svg?branch=master)](https://travis-ci.org/zamblauskas/scala-csv-parser)

About
==============================
CSV parser library for Scala.
Best suited to convert a string representation into a collection of objects.

Usage
==============================

Input CSV string:
``` scala
val csv = """
|name,age,height,city
|Emily,33,195,London
|Thomas,25,,
""".stripMargin
```
Case class we want to convert to:
``` scala
case class Person(name: String, age: Int, city: Option[String])
```

Step 1: define an implicit `ColumnReads[Person]`:
``` scala
implicit val personReads: ColumnReads[Person] = (
  column("name").as[String]    and
  column("age").as[Int]        and
  column("city").asOpt[String]
)(Person)
```

Step 2: get the result:
``` scala
val result: Either[Failure, Seq[Person]] = parse(csv)
println(result)
//Right(List(Person(Emily,33,Some(London)), Person(Thomas,25,None)))
```

SBT dependency
==============================

Add to your `build.sbt`:

``` scala
resolvers += Resolver.bintrayRepo("zamblauskas", "maven")

libraryDependencies += "zamblauskas" %% "scala-csv-parser" % "0.7.0"
```
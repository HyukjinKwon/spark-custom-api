# Custom API Examples For Apache Spark

[![Build Status](https://travis-ci.org/HyukjinKwon/spark-custom-api.svg?branch=master)](https://travis-ci.org/HyukjinKwon/spark-custom-api) [![codecov.io](http://codecov.io/github/HyukjinKwon/spark-custom-api/coverage.svg?branch=master)](http://codecov.io/github/HyukjinKwon/spark-custom-api?branch=master)

The examples are basic and only for newbies in Scala and Spark.

## Motivation

There are a lot of developers who love Spark and want to have custom APIs for Spark. For example, a Spark integration with another software or simple customized APIs, which can be modulized and used frequently as a thrid-party library. For those guys, here are some custom API examples in this library. This can be imported by a jar as a third-party with Spark.

## Custom APIs in this library

- **`customPipe(..)`** equivalent to `pipe(..)` in `RDD`

  ```scala
  import com.company.spark.custom._
  
  sc.parallelize(Seq(1, 2, 3))
  rdd.customPipe("cat").collect()
  ```

- **`customCount()`** equivalent to `count()` in `DataFrame`

  ```scala
  import com.company.spark.custom._
  
  val data = Seq(1, 2, 3, 4, 5)
  val rdd = sc.parallelize(numList)
  val df = numRDD.toDF
  df.customCount()
  ```

- **`customTextFile(..)`** equivalent to `textFile(..)` in `SparkContext`

  ```scala
  import com.company.spark.custom._
  
  val path = "path-to-file"
  sc.customTextFile(path)
  ```

- **`customLoadJsonRDD(..)`** equivalent to `jsonRDD(..)` in `SQLContext`

  ```scala
  import com.company.spark.custom._
  
  val jsonRDD = sparkContext.parallelize(
    """{"a": 1}""" ::
    """{"a": 2}""" :: Nil)
  sqlContext.customLoadJsonRDD(jsonRDD)
  ```

## Test

- For test in your local, just run below:

  ```
  ./dev/run-tests
  ```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.


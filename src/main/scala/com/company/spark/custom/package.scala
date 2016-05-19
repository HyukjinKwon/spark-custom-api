/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.company.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}

package object custom {
  /**
   * Adds a method, `customPipe`, to [[RDD]].
   */
  implicit class CustomRDD[T](rdd: RDD[T]) {
    /**
     * Return an RDD created by piping elements to a forked external process.
     * The print behavior can be customized by providing two functions.
     *
     * @param command command to run in forked process.
     */
    def customPipe(command: String): RDD[String] = {
      rdd.pipe(command)
    }
  }

  /**
   * Adds a method, `customCount`, to [[DataFrame]].
   */
  implicit class CustomDataFrame(df: DataFrame) {
    /**
     * Returns the number of rows in the [[DataFrame]].
     */
    def customCount(): Long = {
      df.count()
    }
  }

  /**
   * Adds a method, `customTextFile`, to [[SparkContext]].
   */
  implicit class CustomSparkContext(sparkContext: SparkContext) {
    /**
     * Read a text file from HDFS, a local file system (available on all nodes), or any
     * Hadoop-supported file system URI, and return it as an RDD of Strings.
     *
     * @param path file path to read.
     */
    def customTextFile(path: String): RDD[String] = {
      sparkContext.textFile(path)
    }
  }

  /**
   * Adds a method, `customLoadJsonRDD`, to [[SQLContext]].
   */
  implicit class CustomSQLContext(sqlContext: SQLContext) {
    /**
     * Return an a [[DataFrame]] from [[RDD]] consist of json data.
     *
     * @param rdd Data having json document in each line.
     */
    def customLoadJsonRDD(rdd: RDD[String]): DataFrame = {
      sqlContext.jsonRDD(rdd)
    }
  }
}

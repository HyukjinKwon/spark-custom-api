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

package com.company.spark.custom

import java.io.{FileWriter, BufferedWriter, File}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SQLImplicits}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkSuite extends FunSuite with BeforeAndAfterAll {
  private var sqlContext: SQLContext = _
  private var sparkContext: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "Custom"))
    sparkContext = sqlContext.sparkContext
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  private def tempTextFile(data: Seq[String])(f: String => Unit): Unit = {
    val temp = File.createTempFile("tmp", ".tmp")
    val bw = new BufferedWriter(new FileWriter(temp))
    data.foreach(line => bw.write(s"$line\n"))
    bw.close()
    try f(temp.getAbsolutePath) finally temp.delete()
  }

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = sqlContext
  }

  test("Simple customPipe() test") {
    val rdd = sparkContext.parallelize(Seq(1, 2, 3))
    val actual = rdd.customPipe("cat").collect()
    val expected = Seq("1", "2", "3")
    assert(actual === expected)
  }

  test("Simple customCount() test") {
    import testImplicits._

    val df = (0 to 1).map(i => (i, i.toString)).toDF()
    assert(df.customCount() == 2)
  }

  test("Simple customTextFile() test") {
    val data = Seq("aa", "bb", "cc")
    tempTextFile(data) { path =>
      val rdd = sparkContext.customTextFile(path)
      val actual = rdd.collect()
      assert(actual === data)
    }
  }

  test("Simple customLoadJsonRDD() test") {
    val jsonRDD = sparkContext.parallelize(
      """{"a": 1}""" ::
      """{"a": 2}""" :: Nil)
    val df = sqlContext.customLoadJsonRDD(jsonRDD)
    assert(df.select("a").head().get(0) === 1)
  }
}

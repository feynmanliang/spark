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

package org.apache.spark.ml.ensemble

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}

class BaggingSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("bagging with logistic regression") {
    val dataset = sqlContext.createDataFrame(
        sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))

    val lr = new LogisticRegression
    val bagging = new Bagging()
      .setEstimator(lr)
      .setIsClassifier(true)
      .setNumModels(3)

    val baggedModel = bagging.fit(dataset)
    baggedModel.transform(dataset)
  }

  test("bagging with linear regression") {
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))

    val lr = new LinearRegression
    val bagging = new Bagging()
      .setEstimator(lr)
      .setIsClassifier(false)
      .setNumModels(3)

    val baggedModel = bagging.fit(dataset)
    baggedModel.transform(dataset)
  }
}

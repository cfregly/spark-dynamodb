/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.dynamodb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType

/**
 * Provides access to DynamoDB data from pure SQL statements.
 */
class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for data store in DynamoDB given a `region` and 'table' within that region as parameters.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    DynamoDBRelation(parameters("region"), parameters("table"))(sqlContext)
  }
}


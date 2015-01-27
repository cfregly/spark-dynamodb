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
package com.databricks.spark

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD

package object dynamodb {

  /**
   * Adds a method, `dynamoDB`, to SQLContext that allows reading data stored in DynamoDB.
   */
  implicit class DynamoDBContext(sqlContext: SQLContext) {
	/**
	 * @param region (ie. us-east-1, us-west-2, etc)
	 * @param unique table name within the region
	 */
    def dynamoDB(region: String, table: String) = {
      sqlContext.baseRelationToSchemaRDD(DynamoDBRelation(region, table)(sqlContext))
    }
  }

  // TODO: Implement me.
  implicit class DynamoDBSchemaRDD(schemaRDD: SchemaRDD) {
	def saveAsDynamoDB(path: String): Unit = {
	  ???
	}
  }
}

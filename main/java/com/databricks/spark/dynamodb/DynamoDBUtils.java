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
package com.databricks.spark.dynamodb;

import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

/**
 * A collection of static functions for working with DynamoDB in Spark SQL
 */
public class DynamoDBUtils {
    /** Returns a Schema RDD for the given DynamoDB info. */
    public static JavaSchemaRDD dynamoDB(JavaSQLContext sqlContext, String path) {
        DynamoDBRelation = new DynamoDBRelation(path, sqlContext.sqlContext());
        return sqlContext.baseRelationToSchemaRDD(relation);
    }
}

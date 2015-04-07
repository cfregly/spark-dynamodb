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

import com.amazonaws.ClientConfiguration;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

/**
 * A collection of static functions for working with DynamoDB in Spark SQL
 */
public class DynamoDBUtils {
    /** Returns a Schema RDD for the given DynamoDB info (region and table). */
    public static DataFrame dynamoDB(SQLContext sqlContext, String region, String table) {
        scala.Option<ClientConfiguration> none = scala.Option.apply(null);
        scala.Option<StructType> none2 = scala.Option.apply(null);
        DynamoDBRelation relation = new DynamoDBRelation(region, table, none, none2, sqlContext);
        return sqlContext.baseRelationToDataFrame(relation);
    }

    /** Returns a Schema RDD for the given DynamoDB info (region and table). */
    public static DataFrame dynamoDB(SQLContext sqlContext, String region, String table, ClientConfiguration clientConfig) {
        scala.Option<ClientConfiguration> clientConfigOption = scala.Option.apply(clientConfig);
        scala.Option<StructType> none2 = scala.Option.apply(null);
        DynamoDBRelation relation = new DynamoDBRelation(region, table, clientConfigOption, none2, sqlContext);
        return sqlContext.baseRelationToDataFrame(relation);
    }

    /** Returns a Schema RDD for the given DynamoDB info (region and table). */
    public static DataFrame dynamoDB(SQLContext sqlContext, String region, String table, StructType schema) {
        scala.Option<ClientConfiguration> none = scala.Option.apply(null);
        scala.Option<StructType> schemaStruct = scala.Option.apply(schema);
        DynamoDBRelation relation = new DynamoDBRelation(region, table, none, schemaStruct, sqlContext);
        return sqlContext.baseRelationToDataFrame(relation);
    }

    /** Returns a Schema RDD for the given DynamoDB info (region and table). */
    public static DataFrame dynamoDB(SQLContext sqlContext, String region, String table,
                                     ClientConfiguration clientConfig, StructType schema) {
        scala.Option<ClientConfiguration> clientConfigOption = scala.Option.apply(clientConfig);
        scala.Option<StructType> schemaStruct = scala.Option.apply(schema);
        DynamoDBRelation relation = new DynamoDBRelation(region, table, clientConfigOption, schemaStruct, sqlContext);
        return sqlContext.baseRelationToDataFrame(relation);
    }
}

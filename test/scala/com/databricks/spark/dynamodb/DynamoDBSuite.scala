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

import org.apache.spark.sql.test._
import org.scalatest.FunSuite
import TestSQLContext._
import com.amazonaws.services.dynamodbv2.util.Tables
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import java.util.HashMap
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput

class DynamoDBSuite extends FunSuite {
  def setup() = {
    /*
     * The DefaultAWSCredentialsProviderChain will ... TODO
     */
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()

    // Createa a DynamoDB client
    val dynamoDB = new AmazonDynamoDBClient(credentials)

    val tableName = "my-favorite-movies-table"

    // Create table if it does not exist yet
    if (Tables.doesTableExist(dynamoDB, tableName)) {
        System.out.println("Table " + tableName + " is already ACTIVE")
    } else {
        // Create a table with a primary hash key named 'name', which holds a string
        val createTableRequest = new CreateTableRequest().withTableName(tableName)
            .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
            .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L))
            val createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription()
        System.out.println("Created Table: " + createdTableDescription)

        // Wait for it to become active
        System.out.println("Waiting for " + tableName + " to become ACTIVE...")
        Tables.waitForTableToBecomeActive(dynamoDB, tableName)
    }

    // Describe our new table
    val describeTableRequest = new DescribeTableRequest().withTableName(tableName)
    val tableDescription = dynamoDB.describeTable(describeTableRequest).getTable()
    System.out.println("Table Description: " + tableDescription)

    // Add an item:  Map<String, AttributeValue>
    var item = newItem("Bill & Ted's Excellent Adventure", 1989, "****", "James")
    var putItemRequest = new PutItemRequest(tableName, item)
    var putItemResult = dynamoDB.putItem(putItemRequest)
    System.out.println("Result: " + putItemResult)

    // Add another item
    item = newItem("Airplane", 1980, "*****", "Billy Bob")
    putItemRequest = new PutItemRequest(tableName, item)
    putItemResult = dynamoDB.putItem(putItemRequest)
    System.out.println("Result: " + putItemResult)
  }

  def newItem(name: String, year: Integer, rating: String, fan: String): HashMap[String, AttributeValue] = {
    val item = new HashMap[String, AttributeValue]
    item.put("name", new AttributeValue(name))
    item.put("year", new AttributeValue(Integer.toString(year)))
    item.put("rating", new AttributeValue(rating))
    item.put("fan", new AttributeValue(fan))

    item  //Map<String, AttributeValue>
  }
  
  test("dsl test") {
  //  val results = TestSQLContext
  //    .avroFile(episodesFile)
  //    .select('title)
  //    .collect()

  //  assert(results.size === 8)
  }

  test("sql test") {
    //sql(
    //  s"""
    //    |CREATE TEMPORARY TABLE avroTable
    //    |USING com.databricks.spark.avro
    //    |OPTIONS (path "$episodesFile")
    //  """.stripMargin.replaceAll("\n", " "))

   // assert(sql("SELECT * FROM avroTable").collect().size === 8)
  }
}

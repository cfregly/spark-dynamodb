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

import java.util.Map

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.immutable.HashMap

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.BooleanType
import org.apache.spark.sql.DataType
import org.apache.spark.sql.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.StringType
import org.apache.spark.sql.StructType
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.sources.PrunedFilteredScan

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.util.Tables


case class DynamoDBRelation(dynamoDBTable: TableDescription)(@transient val sqlContext: SQLContext) extends PrunedFilteredScan {
  /*
   * The DefaultAWSCredentialsProviderChain will ... TODO
   */
  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()

  // Createa a DynamoDB client
  val dynamoDB = new AmazonDynamoDBClient(credentials)

//        //set region??
//        val usWest2 = Region.getRegion(Regions.US_WEST_2)
//        dynamoDB.setRegion(usWest2)
    
  // Convert the DynamoDB TableDescription into a SparkSQL StructType
  val schema: StructType = toSparkSqlSchema(dynamoDBTable) match {
      case schema: StructType => schema
      case other =>
        sys.error(s"DynamoDB table contains datatypes that are not compatible with SparkSQL:  $other")
  }
  
  /**
   * @param requiredColumns - column pruning
   * @param filters - predicate push downs
   */
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Convert Array[Filter] into Map<columnName:String,Condition>     
    val scanFilter = buildScanFilter(filters)
    
    // Perform the scan
    val scanRequest = new ScanRequest(dynamoDBTable.getTableName()).withScanFilter(scanFilter);
    val scanResult = dynamoDB.scan(scanRequest);
    
    val results = scanResult.getItems()

    // Create the resultsRDD from the results by using sqlContext.sparkContext.parallelize(results)
    val resultsRDD = sqlContext.sparkContext.parallelize(results)
    
    // Convert to Rows
    resultsRDD.map { result =>
      val values = (0 until schema.fields.size).map { i =>
        result.get(i)
      }
    	
      Row.fromSeq(values)
    }
  }
  
  def buildScanFilter(filters: Array[Filter]) : Map[String, Condition] = {
    val scanFilter = new HashMap[String, Condition]
 
    for (filter <- filters) {
      filter match {      
		case EqualTo(attribute: String, value: Any) => 
		  scanFilter.put(
		    attribute, new Condition().withComparisonOperator(ComparisonOperator.EQ)
			  .withAttributeValueList(new AttributeValue(value.toString()))
		  )

		case GreaterThan(attribute: String, value: Any) =>
		  scanFilter.put(attribute, 
  		    new Condition().withComparisonOperator(ComparisonOperator.GT)
			  .withAttributeValueList(new AttributeValue(value.toString()))
		  )
		
		case GreaterThanOrEqual(attribute: String, value: Any) =>
  		  scanFilter.put(attribute, 
		    new Condition().withComparisonOperator(ComparisonOperator.GE)
		      .withAttributeValueList(new AttributeValue(value.toString()))
		  )
		  
		case LessThan(attribute: String, value: Any) =>
  		  scanFilter.put(attribute,
		    new Condition().withComparisonOperator(ComparisonOperator.LT)
			  .withAttributeValueList(new AttributeValue(value.toString()))
		  )
		
		case LessThanOrEqual(attribute: String, value: Any) =>
  		  scanFilter.put(attribute,
  		     new Condition().withComparisonOperator(ComparisonOperator.LE)
			  .withAttributeValueList(new AttributeValue(value.toString()))
		  )
      }
    }
    
    return scanFilter
  }
  
  private case class AttributeType(sparkSqlDataType: DataType, nullable: Boolean)
  
  private def toSparkSqlSchema(dynamoDBTable: TableDescription) : Seq[AttributeType] = {
	val attributeDefinitions = dynamoDBTable.getAttributeDefinitions()
    
	attributeDefinitions.toSeq.map( attributeDefinition =>
        attributeDefinition.getAttributeType() match {
       	 case "N" => AttributeType(IntegerType, nullable = false)
         case "S" => AttributeType(StringType, nullable = false)
         case "B" => AttributeType(BooleanType, nullable = false)
         case other => sys.error(s"Unsupported type $other")
       }
    )
  }
}


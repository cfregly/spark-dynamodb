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

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition, ScanRequest, TableDescription}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap


case class DynamoDBRelation(region: String, table: String, clientConf: Option[ClientConfiguration] = None, providedSchema: Option[StructType] = None)(@transient val sqlContextTransient: SQLContext) extends BaseRelation with PrunedFilteredScan {
  /*
   * The DefaultAWSCredentialsProviderChain will ... TODO
   */
  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials

  // Create a DynamoDB client
  val dynamoDB = clientConf match {
    case Some(clientConfiguration)  => new AmazonDynamoDBClient(credentials, clientConfiguration)
    case _                          => new AmazonDynamoDBClient(credentials)
  }
  dynamoDB.setRegion(Regions.fromName(region))
  
  // DynamoDB Table
  val dynamoDBTable = dynamoDB.describeTable(table).getTable

  override def sqlContext: SQLContext = sqlContextTransient

  // Convert the DynamoDB TableDescription into a SparkSQL StructType
  override val schema: StructType = toSparkSqlSchema(dynamoDBTable) match {
      case Some(schema: StructType) => schema
      case other =>
        sys.error(s"DynamoDB table contains datatypes that are not compatible with SparkSQL:  $other")
  }

  override def sizeInBytes: Long = dynamoDBTable.getTableSizeBytes
  
  /**
   * @param requiredColumns - column pruning
   * @param filters - predicate push downs
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Convert Array[Filter] into Map<columnName:String,Condition>     
    val scanFilter = buildScanFilter(filters)
    
    // Perform the scan
    val scanRequest = new ScanRequest(dynamoDBTable.getTableName).withScanFilter(scanFilter)
    val scanResult = dynamoDB.scan(scanRequest)

    val results = scanResult.getItems

    // Create the resultsRDD from the results by using sqlContext.sparkContext.parallelize(results)
    val resultsRDD = sqlContext.sparkContext.parallelize(results)

    // Convert to Rows
    resultsRDD.flatMap(result => {
      val rowValues = requiredColumns.zipWithIndex.flatMap{ case (requiredColumn, columnIndex) => {
        val columnValue = result.get(requiredColumn)
        (columnValue, providedSchema) match {
          case (null, _)          => None
          case (_, Some(schema))  => DynamoDBItemTools.getCastAttributeFromSchema(columnValue, schema(columnIndex))
          case (_, _)             => DynamoDBItemTools.getCastAttribute(columnValue)
        }
      }}
      if (rowValues.length == requiredColumns.length) Some(Row.fromSeq(rowValues.toSeq)) else None
    })
  }

  def buildScanFilter(filters: Array[Filter]) : Map[String, Condition] = {
    val scanFilter = new HashMap[String, Condition]
 
    for (filter <- filters) {
      filter match {      
		case EqualTo(attribute: String, value: Any) => 
		  scanFilter.put(
		    attribute, new Condition().withComparisonOperator(ComparisonOperator.EQ)
			  .withAttributeValueList(new AttributeValue(value.toString))
		  )

		case GreaterThan(attribute: String, value: Any) =>
		  scanFilter.put(attribute, 
  		    new Condition().withComparisonOperator(ComparisonOperator.GT)
			  .withAttributeValueList(new AttributeValue(value.toString))
		  )
		
		case GreaterThanOrEqual(attribute: String, value: Any) =>
  		  scanFilter.put(attribute, 
		    new Condition().withComparisonOperator(ComparisonOperator.GE)
		      .withAttributeValueList(new AttributeValue(value.toString))
		  )
		  
		case LessThan(attribute: String, value: Any) =>
  		  scanFilter.put(attribute,
		    new Condition().withComparisonOperator(ComparisonOperator.LT)
			  .withAttributeValueList(new AttributeValue(value.toString))
		  )
		
		case LessThanOrEqual(attribute: String, value: Any) =>
  		  scanFilter.put(attribute,
  		     new Condition().withComparisonOperator(ComparisonOperator.LE)
			  .withAttributeValueList(new AttributeValue(value.toString))
		  )
      }
    }
    
    return scanFilter
  }
  
  private case class AttributeType(sparkSqlDataType: DataType, nullable: Boolean)
  
  private def toSparkSqlSchema(dynamoDBTable: TableDescription) : Option[StructType] = {
    providedSchema match {
      case Some(p)  => providedSchema
      case _        => {
        val attributeDefinitions = dynamoDBTable.getAttributeDefinitions
        val keyNames = dynamoDBTable.getKeySchema.map(_.getAttributeName)

        val structFieldArray = attributeDefinitions.flatMap( attributeDefinition => {
          val isKeyField = keyNames.contains(attributeDefinition.getAttributeName)
          attributeDefinition.getAttributeType match {
            case "N" => Some(StructField(attributeDefinition.getAttributeName, LongType, nullable = isKeyField))
            case "S" => Some(StructField(attributeDefinition.getAttributeName, StringType, nullable = isKeyField))
            case "B" => Some(StructField(attributeDefinition.getAttributeName, BooleanType, nullable = isKeyField))
            case other => {
              sys.error(s"Unsupported type $other")
              None
            }
          }
        })
        if (structFieldArray.isEmpty) None else Some(StructType(structFieldArray))
      }
    }
  }
}


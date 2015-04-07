package com.databricks.spark.dynamodb

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

class DynamoDBItemToolsSuite extends FunSuite {

  test("numeric string cast test") {
    assert(NumericStringTools.castNumericStringToNumeric("37") match {
      case Some(i: Int) => true
      case _            => false
    })

    assert(NumericStringTools.castNumericStringToNumeric("3734643643636346") match {
      case Some(l: Long)  => true
      case _              => false
    })

    assert(NumericStringTools.castNumericStringToNumeric("37.346") match {
      case Some(d: Double)  => true
      case _                => false
    })
  }


  test("attributetype cast test") {
    assert(Some("test string").equals(DynamoDBItemTools.getCastAttribute(new AttributeValue("test string"))))
    assert(Some(37).equals(DynamoDBItemTools.getCastAttribute( new AttributeValue().withN("37"))))
    assert(Some(3734643643636346L).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue().withN("3734643643636346"))))
    assert(Some(3.2).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue().withN("3.2"))))
    assert(Some(false).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue().withBOOL(false))))
    assert(Some(List("this", "test")).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue()
      .withL(List(new AttributeValue("this"),new AttributeValue("test"))))))
    assert(Some(List("this", 37)).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue()
      .withL(List(new AttributeValue("this"),new AttributeValue().withN("37"))))))
    assert(Some(List(3, 4)).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue().withNS("3", "4"))))

    val byteBuffer0 = java.nio.ByteBuffer.wrap("test".getBytes)
    val byteBuffer1 = java.nio.ByteBuffer.wrap("this".getBytes)
    assert(Some(byteBuffer0).equals(DynamoDBItemTools.getCastAttribute(new AttributeValue().withB(byteBuffer0))))

    val byteBuffferList = DynamoDBItemTools
      .getCastAttribute(new AttributeValue().withBS(byteBuffer0, byteBuffer1))
      .get.asInstanceOf[java.util.Collection[Any]].toList
    assert(List(byteBuffer0, byteBuffer1).equals(byteBuffferList))
  }

  test("attributetype with schema cast test") {
    val schema = StructType(Array(
      StructField("stringType", StringType, nullable = false),
      StructField("longType", LongType, nullable = false),
      StructField("intType", IntegerType, nullable = false),
      StructField("doubleType", DoubleType, nullable = false),
      StructField("byteBufferType", BinaryType, nullable = false),
      StructField("boolType", BooleanType, nullable = false)
    ))

    assert(Some("37").equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue("37"), schema(0))))
    assert(Some(37L).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withN("37"), schema(1))))
    assert(Some(37).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withN("37"), schema(2))))
    assert(Some(37.0).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withN("37.0"), schema(3))))
    val byteBuffer = java.nio.ByteBuffer.wrap("test".getBytes)
    assert(Some(byteBuffer).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withB(byteBuffer), schema(4))))
    assert(Some(true).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withBOOL(true), schema(5))))

    assert(None.equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withBOOL(true), schema(1))))
    assert(None.equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withN("35.3"), schema(2))))
    assert(Some(35.0).equals(DynamoDBItemTools.getCastAttributeFromSchema(new AttributeValue().withN("35"), schema(3))))
  }

}

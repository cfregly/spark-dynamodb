package com.databricks.spark.dynamodb

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

object DynamoDBItemTools extends Serializable {
  def getCastAttribute(attributeValue: AttributeValue): Option[Any] = {
    if (attributeValue.getS != null)
      Some(attributeValue.getS)
    else if (attributeValue.getN != null)
      NumericStringTools.castNumericStringToNumeric(attributeValue.getN)
    else if (attributeValue.getBOOL != null)
      Some(attributeValue.getBOOL)
    else if (attributeValue.getB != null)
      Some(attributeValue.getB)
    else if (attributeValue.getL != null)
      Some(attributeValue.getL.flatMap(getCastAttribute).toList)
    else if (attributeValue.getBS != null)
      Some(attributeValue.getBS)
    else if (attributeValue.getSS != null)
      Some(attributeValue.getSS)
    else if (attributeValue.getNS != null)
      Some(attributeValue.getNS.flatMap(NumericStringTools.castNumericStringToNumeric).toList)
    else if (attributeValue.getM != null)
      Some(attributeValue.getM.flatMap(x => getCastAttribute(x._2) match {
        case Some(a)  => Some(x._1 -> a)
        case _        => None
      }).toMap)
    else
      None
  }

  def getCastAttributeFromSchema(columnValue: AttributeValue, structField: StructField) = {
    try {
      structField.dataType match {
        case s: StringType => Some(columnValue.getS)
        case b: BooleanType => Some(columnValue.getBOOL)
        case d: DoubleType => Some(columnValue.getN.toDouble)
        case i: IntegerType => Some(columnValue.getN.toInt)
        case l: LongType => Some(columnValue.getN.toLong)
        case b: BinaryType => Some(columnValue.getB)
        case f: FloatType => Some(columnValue.getN.toFloat)
        case _ => getCastAttribute(columnValue)
      }
    } catch {
      case n: NumberFormatException => None
      case cc: ClassCastException   => None
    }
  }
}

object NumericStringTools extends Serializable {
  def castNumericStringToNumeric(numericString: String): Option[Any] = {
    val intValOption = NumericStringTools.castNumericStringToInt(numericString)
    intValOption match {
      case None   =>
        val longValOption = NumericStringTools.castNumericStringToLong(numericString)
        longValOption match {
          case None =>
            val doubleValOption = NumericStringTools.castNumericStringToDouble(numericString)
            doubleValOption match {
              case None => None
              case _    => doubleValOption
            }
          case _    => longValOption
        }
      case _      => intValOption
    }
  }

  def castNumericStringToInt(numericStr: String): Option[scala.Int] = {
    try {
      Some(numericStr.toInt)
    } catch {
      case nfe: NumberFormatException => {
        None
      }
    }
  }

  def castNumericStringToLong(numericStr: String): Option[scala.Long] = {
    try {
      Some(numericStr.toLong)
    } catch {
      case nfe: NumberFormatException => {
        None
      }
    }
  }

  def castNumericStringToDouble(numericStr: String): Option[scala.Double] = {
    try {
      Some(numericStr.toDouble)
    } catch {
      case nfe: NumberFormatException => {
        None
      }
    }
  }


}
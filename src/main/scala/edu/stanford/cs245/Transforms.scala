package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.{Add, And, BinaryComparison, DecimalLiteral, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF, Subtract}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DataType, Decimal, DoubleType, IntegerType, IntegralType, NumericType}

object Transforms {

  // check if a ScalaUDF Expression is our dist UDF
  def isDistUdf(udf: ScalaUDF): Boolean = {
    udf.udfName.getOrElse("") == "dist"
  }

  // get an Expression representing the dist_sq UDF with the provided
  // arguments
  def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Double, y1: Double, x2: Double, y2: Double) => {
        val xDiff = x1 - x2;
        val yDiff = y1 - y2;
        xDiff * xDiff + yDiff * yDiff
      }, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
      udfName = Option.apply("dist_sq"))
  }

  // Return any additional optimization passes here
  def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(EliminateZeroDists(spark),
      ReorderExpressions(spark),
      SimplifyDistWithZeroComparison(spark),
      NegativeConstantComparison(spark),
      SquareTransform(spark))
  }

  case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
        udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
    }
  }

  case class ReorderExpressions(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case e @ BinaryComparison(l: ScalaUDF, r: ScalaUDF) if isDistUdf(l) && isDistUdf(r) => e
      case GreaterThan(l, udf: ScalaUDF) if isDistUdf(udf) => LessThan(udf, l)
      case LessThan(l, udf: ScalaUDF) if isDistUdf(udf) => GreaterThan(udf, l)
      case GreaterThanOrEqual(l, udf: ScalaUDF) if isDistUdf(udf) => LessThanOrEqual(udf, l)
      case LessThanOrEqual(l, udf: ScalaUDF) if isDistUdf(udf) => GreaterThanOrEqual(udf, l)
      case EqualTo(l, udf: ScalaUDF) if isDistUdf(udf) => EqualTo(udf, l)
    }
  }

  case class SimplifyDistWithZeroComparison(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case e @ EqualTo(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        if (isZero(r))
          EqualTo(Add(Subtract(udf.children(2), udf.children(0)), Subtract(udf.children(3), udf.children(1))), r)
        else
          e
      }
    }

    def isZero(i: Literal): Boolean = {
      i.value match {
        case v: Double => v == 0
        case v: Float => v == 0
        case v: Integer => v == 0
        case _ => false
      }
    }
  }

  case class NegativeConstantComparison(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions   {
      case e @ EqualTo(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        r.value match {
          case v: Double => if (v < 0) FalseLiteral else e
          case v: Float => if (v < 0) FalseLiteral else e
          case v: Integer => if (v < 0) FalseLiteral else e
        }
      }
      case e @ LessThan(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        r.value match {
          case v: Double => if (v <= 0) FalseLiteral else e
          case v: Float => if (v <= 0) FalseLiteral else e
          case v: Integer => if (v <= 0) FalseLiteral else e
        }
      }
      case e @ LessThanOrEqual(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        r.value match {
          case v: Double => if (v < 0) FalseLiteral else e
          case v: Float => if (v < 0) FalseLiteral else e
          case v: Integer => if (v < 0) FalseLiteral else e
        }
      }
      case e @ GreaterThan(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        r.value match {
          case v: Double => if (v < 0) TrueLiteral else e
          case v: Float => if (v < 0) TrueLiteral else e
          case v: Integer => if (v < 0) TrueLiteral else e
        }
      }
      case e @ GreaterThanOrEqual(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType] => {
        r.value match {
          case v: Double => if (v < 0) TrueLiteral else e
          case v: Float => if (v < 0) TrueLiteral else e
          case v: Integer => if (v < 0) TrueLiteral else e
        }
      }
    }
  }

  case class SquareTransform(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {

      case e @ BinaryComparison(udf: ScalaUDF, r: Literal) if isDistUdf(udf) && r.dataType.isInstanceOf[NumericType]
        && isPositive(r) => {
        e match {
          case GreaterThan(udf: ScalaUDF, r: Literal) => GreaterThan(sq(udf), sq(r))
          case LessThan(udf: ScalaUDF, r: Literal) => LessThan(sq(udf), sq(r))
          case GreaterThanOrEqual(udf: ScalaUDF, r: Literal) => GreaterThanOrEqual(sq(udf), sq(r))
          case LessThanOrEqual(udf: ScalaUDF, r: Literal) => LessThanOrEqual(sq(udf), sq(r))
          case EqualTo(udf: ScalaUDF, r: Literal) => EqualTo(sq(udf), sq(r))
        }
      }
      case e @ BinaryComparison(l: ScalaUDF, r: ScalaUDF) if isDistUdf(l) && isDistUdf(r) => {
        e match {
          case GreaterThan(l: ScalaUDF, r: ScalaUDF) => GreaterThan(sq(l), sq(r))
          case LessThan(l: ScalaUDF, r: ScalaUDF) => LessThan(sq(l), sq(r))
          case GreaterThanOrEqual(l: ScalaUDF, r: ScalaUDF) => GreaterThanOrEqual(sq(l), sq(r))
          case LessThanOrEqual(l: ScalaUDF, r: ScalaUDF) => LessThanOrEqual(sq(l), sq(r))
          case EqualTo(l: ScalaUDF, r: ScalaUDF) => EqualTo(sq(l), sq(r))
        }
      }
    }


    def sq(i: ScalaUDF): ScalaUDF = getDistSqUdf(i.children)

    def sq(i: Literal): Literal = {
      i.value match {
        case v: Double => Literal(math.pow(v, 2), DoubleType)
        case v: Float => Literal(math.pow(v, 2), DoubleType)
        case v: Integer => Literal(math.pow(v.doubleValue(), 2), DoubleType)
      }
    }

    def isPositive(i: Literal): Boolean = {
      i.value match {
        case v: Double => v >= 0
        case v: Float => v >= 0
        case v: Integer => v >= 0
        case _ => false
      }
    }
  }

}

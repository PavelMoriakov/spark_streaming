package functions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import Constants._

object SparkStreaminMain {



  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Word Count")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation",true)
      .getOrCreate()

    val inputDfSchema: StructType = StructType(
      StructField(LETTER_COL, StringType, false) ::
        StructField(TIMESTAMP_COL, TimestampType, false) ::
        Nil)

    val mappingDfSchema: StructType = StructType(StructField(LETTER_COL, StringType, false) ::
      StructField(COEFFICIENT_COL, IntegerType, false) ::
      Nil)


    val mappingDF: DataFrame = spark.read
      .schema(mappingDfSchema)
      .csv("/Users/pavel/My_files/Finastra/Homework/mappings.csv")


    val initDF: DataFrame = spark
      .readStream
      .format("csv")
      .option("path", "/Users/pavel/My_files/Finastra/Homework/test/")
      .option("timestampFormat","'D'dd-MM-yyyy'T'HH:mm:ss")
      .schema(inputDfSchema)
      .load()

    initDF.printSchema()

    val dataFrame: DataFrame = initDF.join(mappingDF,LETTER_COL)
      //.withColumn(TIMESTAMP_COL,current_timestamp())
      //.withWatermark(TIMESTAMP_COL, WINDOW_DURATION)
      .groupBy(col(LETTER_COL), window(col(TIMESTAMP_COL), WINDOW_DURATION))
      .agg(count(col(LETTER_COL)) as "count")

    dataFrame.printSchema()


    val df: DataFrame = dataFrame.join(mappingDF, LETTER_COL)
      .withColumn(SUM_COL, col(COUNT_COL) * col(COEFFICIENT_COL))
      .withColumn(TIME_WINDOW_COL, concat(date_format(col("window.start"), HHMMSS_DATE_FORMAT),
        lit("-"), date_format(col("window.end"), HHMMSS_DATE_FORMAT)))
      .select(LETTER_COL, TIME_WINDOW_COL, SUM_COL)

    df.printSchema()

    /*When i switch from console to append mode and uncomment lines '55' & '56'
    * result didn't match with expectation, because of watermark has same column name as timestamp column
    * and groupBy doesn't work properly
    * I can't find solution how to solve the issue
    * */

    /*Switch to append mode*/

//    df.writeStream
//      .outputMode("append")
//      .format("csv")
//      .option("path", "/Users/pavel/My_files/Finastra/Homework/output/")
//      .option("checkpointLocation", "/Users/pavel/My_files/Finastra/Homework/checkpoint")
//      .start()
//      .awaitTermination()

    df.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", 100)
      .start()
      .awaitTermination()
  }

}

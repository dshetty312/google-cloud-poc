package com.spark.test.SparkETL

import org.apache.spark.sql.SparkSession


import org.apache.spark.sql.SaveMode;

object ETLJob {
  def main(args : Array[String]) {
   
  val spark = SparkSession.builder().appName("spark-gcp-etl").getOrCreate()
  
  val dfLoans=spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("gs://spark-input");
  
   dfLoans.printSchema();
        // Creates temporary view using DataFrame
        dfLoans.withColumnRenamed("Country", "country")
                .withColumnRenamed("Country Code", "country_code")
                .withColumnRenamed("Disbursed Amount", "disbursed")
                .withColumnRenamed("Borrower's Obligation", "obligation")
                .withColumnRenamed("Interest Rate", "interest_rate")
                .createOrReplaceTempView("loans");
  
   val dfDisbursement = spark.sql(
                "SELECT country, country_code, "
                        + "format_number(total_disbursement, 0) AS total_disbursement, "
                        + "format_number(ABS(total_obligation), 0) AS total_obligation, "
                        + "format_number(avg_interest_rate, 2) AS avg_interest_rate "
                        + "FROM ( "
                        + "SELECT country, country_code, "
                        + "SUM(disbursed) AS total_disbursement, "
                        + "SUM(obligation) AS total_obligation, "
                        + "AVG(interest_rate) AS avg_interest_rate "
                        + "FROM loans "
                        + "GROUP BY country, country_code "
                        + "ORDER BY total_disbursement DESC "
                        + "LIMIT 25)"
        );
    dfDisbursement.show(25, 100);
    dfDisbursement.repartition(1)
                .write
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save("gs://spark-test-output/ibrd-loan-summary");
    
        System.out.println("Results successfully written to CSV file");
  }
}
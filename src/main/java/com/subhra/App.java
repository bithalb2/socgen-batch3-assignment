package com.subhra;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Spark app").getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("data/airlines.csv");

        dataset.createOrReplaceTempView("airlines");

        /* 1) What was the highest number of people travelled in which year? */
        dataset.groupBy(col("year"))
                .agg(sum(col("total_no_of_booked_seats")).as("no_of_people_travelled"))
                .orderBy(desc("no_of_people_travelled")).limit(1).show();

        /* 2) Identifying the highest revenue generation for which year? */
        dataset.groupBy(col("year"))
                .agg(sum(col("average_revenue_per_seat").multiply(col("total_no_of_booked_seats")))
                        .cast("Decimal(10,2)").as("average_revenue_per_year")
                ).orderBy(desc("average_revenue_per_year")).limit(1).show();

        /* 3) Identifying the highest revenue generation for which year and quarter (Common group)? */
        dataset.groupBy(col("year"), col("quarter"))
                .agg(sum(col("average_revenue_per_seat").multiply(col("total_no_of_booked_seats")))
                        .cast("Decimal(10,2)").as("average_revenue_per_quarter")
                ).orderBy(desc("average_revenue_per_quarter")).limit(1).show();

    }
}

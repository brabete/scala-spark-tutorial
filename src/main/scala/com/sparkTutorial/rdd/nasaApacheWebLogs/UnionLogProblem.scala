package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    Logger.getLogger("org").setLevel(Level.INFO)

    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val julyLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregatedLogs = augustLogs.union(julyLogs)

    val cleanLogs = aggregatedLogs.filter(line => inNotHeader(line))

    val sample = cleanLogs.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.tsv")
  }

  def inNotHeader(str: String): Boolean = !(str.startsWith("host") && str.contains("response"))
}

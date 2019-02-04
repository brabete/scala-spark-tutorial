package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("airportsInUSA").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val airpots = sc.textFile("in/airports.text")
    val airportsInUSA = airpots.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    val airpotsNameAndCity = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })

    airpotsNameAndCity.saveAsTextFile("out/airports_in_usa.text")

    for(airport <- airpotsNameAndCity) println(airport)
  }
}

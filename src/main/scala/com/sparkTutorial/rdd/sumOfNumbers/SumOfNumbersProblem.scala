package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("sumOfNum").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val primeNum = sc.textFile("in/prime_nums.text")

    val numbers = primeNum.flatMap(line => line.split("\\s+"))

    val validNumbers = numbers.filter(number => !number.isEmpty)

    val intNumbers = validNumbers.map(number => number.toInt)

    val sumOfNumbers = intNumbers.reduce((x, y) => x + y)

    println("Sum of Numbers: " + sumOfNumbers)
  }
}

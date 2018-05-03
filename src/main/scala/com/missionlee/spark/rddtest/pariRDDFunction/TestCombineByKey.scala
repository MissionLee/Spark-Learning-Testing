package com.missionlee.spark.rddtest.pariRDDFunction

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 18/04/18 16:11
  * @Description:
  * @Modified by:
  */
object TestCombineByKey {
  val ss = SparkSession.builder().master("local").appName("123").getOrCreate()
  val sc = ss.sparkContext
  //type alias for tuples, increases readablity
  type ScoreCollector = (Int, Double)
  type PersonScores = (String, (Int, Double))

  val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

  val wilmaAndFredScores = sc.parallelize(initialScores).cache()

  val createScoreCombiner = (score: Double) => (1, score)

  val scoreCombiner = (collector: ScoreCollector, score: Double) => {
    val (numberScores, totalScore) = collector
    (numberScores + 1, totalScore + score)
  }

  val scoreMerger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
    val (numScores1, totalScore1) = collector1
    val (numScores2, totalScore2) = collector2
    (numScores1 + numScores2, totalScore1 + totalScore2)
  }
  val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

  val averagingFunction = (personScore: PersonScores) => {
    val (name, (numberScores, totalScore)) = personScore
    (name, totalScore / numberScores)
  }

  val averageScores = scores.collectAsMap().map(averagingFunction)



  def main(args: Array[String]): Unit = {
    println("Average Scores using CombingByKey")
    averageScores.foreach((ps) => {
      val(name,average) = ps
      println(name+ "'s average score : " + average)
    })
  }
}

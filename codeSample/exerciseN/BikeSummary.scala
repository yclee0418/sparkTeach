package bike

//spark lib
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

//log
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j._

//summary statistics
import org.apache.spark.mllib.linalg._
//TODO: 引入兩個所需的Object／Class
import org.apache.spark.mllib.stat.{  }

object BikeSummary {
  //TODO: 完成Entity的Class定義
  case class BikeShareEntity()
  
  def main(args: Array[String]): Unit = {
    setLogger
    //TODO: 初始化SparkContext
    val sc = 

    println("============== preparing data ==================")
    val bikeData = prepare(sc)
    bikeData.persist()
    println("============== print summary statistics ==================")
    printSummary(bikeData)
    println("============== print Correlation ==================")
    printCorrelation(bikeData)
    bikeData.unpersist()
  }

  def prepare(sc: SparkContext): RDD[BikeShareEntity] = {
    val rawDataWithHead = sc.textFile("data/hour.csv")
    val rawDataNoHead = rawDataWithHead.mapPartitionsWithIndex { (idx, iter) => { if (idx == 0) iter.drop(1) else iter } }
    val rawData = rawDataNoHead.map { x => x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map { x => x.trim() } }

    println("read BikeShare Dateset count=" + rawData.count())
    //TODO: 透過map來建立RDD[BikeShareEntity]
    val bikeData = 
    bikeData
  }
  
  def getFeatures(bikeData: BikeShareEntity): Array[Double] = {
    //TODO: 完成getFeatures方法實作
  }

  def printSummary(entRdd: RDD[BikeShareEntity]) = {
    //TODO: 完成printSummary方法實作

  }

  def printCorrelation(entRdd: RDD[BikeShareEntity]) = {
    //TODO: 完成printCorrelation方法實作
    
  }
  
  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)//mark for MLlib INFO msg
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("io").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.ALL);
  }
  
  /*
   * 四拾五入
   */
  def round(input: Double, digit:Int) : Double = {
    val res = BigDecimal(input).setScale(digit, BigDecimal.RoundingMode.HALF_UP).toDouble
    res
  }
}
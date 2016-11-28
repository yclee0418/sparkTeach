package bike

//spark lib
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

//log
import org.apache.log4j._

//summary statistics
import org.apache.spark.mllib.linalg._

//clusting
//TODO: 引入兩個所需的Object／Class
import org.apache.spark.mllib.clustering.{  }

object BikeShareClustering {
  case class BikeShareEntity(instant: String,	dteday:String,	season:Double,	yr:Double,	mnth:Double,
      hr:Double,	holiday:Double,	weekday:Double,	workingday:Double,	weathersit:Double,	temp:Double,
      atemp:Double,	hum:Double,	windspeed:Double,	casual:Double,	registered:Double,	cnt:Double)
      
  def main(args: Array[String]): Unit = {
    setLogger
    //TODO: 初始化SparkContext
    val sc = 

    println("============== preparing data ==================")
    val bikeData = prepare(sc)
    bikeData.persist()
    println("============== clusting by KMeans ==================")
    //TODO: 以初始參數執行KMeans
    
    //TODO: 印出各群組中心點
    println("============== tuning parameters ==================")
    //TODO: 反覆嘗試不同的K值，參考WCSS及群組中心的觀察，決定使用的K值
    
    bikeData.unpersist()
  }

  def prepare(sc: SparkContext): RDD[BikeShareEntity] = {
    val rawDataWithHead = sc.textFile("data/hour.csv")
    val rawDataNoHead = rawDataWithHead.mapPartitionsWithIndex { (idx, iter) => { if (idx == 0) iter.drop(1) else iter } }
    val rawData = rawDataNoHead.map { x => x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map { x => x.trim() } }

    println("read BikeShare Dateset count=" + rawData.count())
    val bikeData = rawData.map { x => BikeShareEntity(
     x(0), x(1), x(2).toDouble, x(3).toDouble,x(4).toDouble, 
      x(5).toDouble,	x(6).toDouble,	x(7).toDouble,	x(8).toDouble,	x(9).toDouble,	x(10).toDouble,    
      x(11).toDouble,	x(12).toDouble,	x(13).toDouble,	x(14).toDouble,	x(15).toDouble,	x(16).toDouble) }
    bikeData
  }
  
  def getFeatures(bikeData: BikeShareEntity): Array[Double] = {
    //TODO: 完成getFeatures方法實作
  }
  
  def getDisplayString(centers:Array[Double]): String = {
    val dispStr = """cnt: %.5f, yr: %.5f, season: %.5f, mnth: %.5f, hr: %.5f, holiday: %.5f, 
      weekday: %.5f, workingday: %.5f, weathersit: %.5f, temp: %.5f, atemp: %.5f, hum: %.5f, 
      windspeed: %.5f, casual: %.5f, registered: %.5f"""
      .format(centers(0), centers(1),centers(2), centers(3),centers(4), centers(5),centers(6), centers(7),
          centers(8), centers(9),centers(10), centers(11),centers(12), centers(13),centers(14))
    dispStr
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
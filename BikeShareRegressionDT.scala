package bike

//time lib
import org.joda.time._
//spark lib
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

//log
import org.apache.log4j._

//MLlib lib
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.linalg.Vectors
//decision tree
//TODO: import Decision Tree所需之Library
import org.apache.spark.mllib.regression.
import org.apache.spark.mllib.tree.
import org.apache.spark.mllib.tree.model.

object BikeShareRegressionDT {
  case class BikeShareEntity(instant: String, dteday: String, season: Double, yr: Double, mnth: Double,
                             hr: Double, holiday: Double, weekday: Double, workingday: Double, weathersit: Double, temp: Double,
                             atemp: Double, hum: Double, windspeed: Double, casual: Double, registered: Double, cnt: Double)
  def main(args: Array[String]): Unit = {
    setLogger
    val doTrain = (args != null && args.length > 0 && "Y".equals(args(0)))
    val sc = new SparkContext(new SparkConf().setAppName("BikeRegressionDT").setMaster("local[*]"))

    println("============== preparing data ==================")
    val (trainData, validateData) = prepare(sc)
    trainData.persist(); validateData.persist();

    val cateInfo = getCategoryInfo()
    if (!doTrain) {
      println("============== train Model (CateInfo)==================")
      val (modelC, durationC) = trainModel(trainData, "variance", 5, 30, cateInfo)
      val rmseC = evaluateModel(validateData, modelC)
      println("validate rmse(CateInfo)=%f".format(rmseC))
    } else {
      println("============== tuning parameters(CateInfo) ==================")
      tuneParameter(trainData, validateData, cateInfo)
    }

    trainData.unpersist(); validateData.unpersist();
  }

  def prepare(sc: SparkContext): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val rawDataWithHead = sc.textFile("data/hour.csv")
    val rawDataNoHead = rawDataWithHead.mapPartitionsWithIndex { (idx, iter) => { if (idx == 0) iter.drop(1) else iter } }
    val rawData = rawDataNoHead.map { x => x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map { x => x.trim() } }

    println("read BikeShare Dateset count=" + rawData.count())
    val bikeData = rawData.map { x =>
      BikeShareEntity(x(0), x(1), x(2).toDouble, x(3).toDouble, x(4).toDouble,
        x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble,
        x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble, x(15).toDouble, x(16).toDouble)
    }
    
    val lpData = bikeData.map { x =>
      {
        //TODO：完成產生LabeledPoint的邏輯
      }
    }
    //以6:4的比例隨機分割，將資料切分為訓練及驗證用資料
    //TODO: 實作6:4的比例隨機分割，產生trainData及 validateData
    (trainData, validateData)
  }
  
  def getFeatures(bikeData: BikeShareEntity): Array[Double] = {
    //TODO: 完成getFeatures方法實作(提醒：部份類別型變數欄位值要－1)
  }
  
  def getCategoryInfo(): Map[Int, Int] = {
    //("yr", 2), ("season", 4), ("mnth", 12), ("hr", 24),
    //("holiday", 2), ("weekday", 7), ("workingday", 2), ("weathersit", 4)
    //TODO： 建立categroyInfoMap，存入類別變數資訊並回傳
    
  }

  def trainModel(trainData: RDD[LabeledPoint],
                 impurity: String, maxDepth: Int, maxBins: Int, catInfo: Map[Int, Int]): (DecisionTreeModel, Double) = {
    val startTime = new DateTime()
    //TODO: 實作訓練DecisionTreeModel邏輯
    
    val endTime = new DateTime()
    val duration = new Duration(startTime, endTime)
    //MyLogger.debug(model.toDebugString)
    (model, duration.getMillis)
  }

  def evaluateModel(validateData: RDD[LabeledPoint], model: DecisionTreeModel): Double = {
    //TODO： 實作評估model邏輯
  }

  def tuneParameter(trainData: RDD[LabeledPoint], validateData: RDD[LabeledPoint], cateInfo:Map[Int,Int]) = {
    val impurityArr = Array("variance")
    val depthArr = Array(3, 5, 10, 15, 20, 25)
    val binsArr = Array(/*3, 5, 10,*/ 50, 100, 200)
    val evalArr =
      //TODO: 實作評估不同參數組合，並選出最佳參數組合邏輯
    val bestEvalAsc = (evalArr.sortBy(_._4))
    val bestEval = bestEvalAsc(0)
    println("best parameter: impurity=%s, maxDepth=%d, maxBins=%d, rmse=%f"
      .format(bestEval._1, bestEval._2, bestEval._3, bestEval._4))
  }
  
  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)//mark for MLlib INFO msg
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("io").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.ALL);
  }
}
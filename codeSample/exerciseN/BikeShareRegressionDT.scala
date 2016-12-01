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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

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
        val label = x.cnt
        val features = Vectors.dense(getFeatures(x))
        new LabeledPoint(label, features) //LabeledPoint由label及Vector組成
      }
    }
    //以6:4的比例隨機分割，將資料切分為訓練及驗證用資料
    val Array(trainData, validateData) = lpData.randomSplit(Array(0.6, 0.4))
    (trainData, validateData)
  }
  
  def getFeatures(bikeData: BikeShareEntity): Array[Double] = {
    val featureArr = Array(bikeData.yr, bikeData.season - 1, bikeData.mnth - 1, bikeData.hr,
      bikeData.holiday, bikeData.weekday, bikeData.workingday, bikeData.weathersit - 1, bikeData.temp, bikeData.atemp,
      bikeData.hum, bikeData.windspeed)
    featureArr
  }
  
  def getCategoryInfo(): Map[Int, Int] = {
    //("yr", 2), ("season", 4), ("mnth", 12), ("hr", 24),
    //("holiday", 2), ("weekday", 7), ("workingday", 2), ("weathersit", 4)
    val categoryInfoMap = Map[Int, Int](( /*"yr"*/ 0, 2), ( /*season*/ 1, 4), ( /*"mnth"*/ 2, 12), ( /*"hr"*/ 3, 24),
      ( /*"holiday"*/ 4, 2), ( /*"weekday"*/ 5, 7), ( /*"workingday"*/ 6, 2), ( /*"weathersit"*/ 7, 4))
    //val categoryInfoMap = Map[Int, Int]()
    categoryInfoMap
  }

  def trainModel(trainData: RDD[LabeledPoint],
                 impurity: String, maxDepth: Int, maxBins: Int, catInfo: Map[Int, Int]): (DecisionTreeModel, Double) = {
    val startTime = new DateTime()
    
    val model = DecisionTree.trainRegressor(trainData, catInfo, impurity, maxDepth, maxBins)
    val endTime = new DateTime()
    val duration = new Duration(startTime, endTime)
    //MyLogger.debug(model.toDebugString)
    (model, duration.getMillis)
  }

  def evaluateModel(validateData: RDD[LabeledPoint], model: DecisionTreeModel): Double = {
    val scoreAndLabels = validateData.map { data =>
      var predict = model.predict(data.features)
      (predict, data.label)
    }
    val metrics = new RegressionMetrics(scoreAndLabels)
    val rmse = metrics.rootMeanSquaredError
    rmse
  }

  def tuneParameter(trainData: RDD[LabeledPoint], validateData: RDD[LabeledPoint], cateInfo:Map[Int,Int]) = {
    val impurityArr = Array("variance")
    val depthArr = Array(3, 5, 10, 15, 20, 25)
    val binsArr = Array(/*3, 5, 10,*/ 50, 100, 200)
    val evalArr =
      for (impurity <- impurityArr; maxDepth <- depthArr; maxBins <- binsArr) yield {
        val (model, duration) = trainModel(trainData, impurity, maxDepth, maxBins, cateInfo)
        val rmse = evaluateModel(validateData, model)
        println("parameter: impurity=%s, maxDepth=%d, maxBins=%d, rmse=%f"
          .format(impurity, maxDepth, maxBins, rmse))
        (impurity, maxDepth, maxBins, rmse)
      }
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
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

//Logistic
//TODO: import LogisticRegression所需之Library
import org.apache.spark.mllib.regression.
import org.apache.spark.mllib.feature.
import org.apache.spark.mllib.classification.{  }

/**
 * BikeShare predict by LogisticRegression
 */
object BikeShareClassificationLG {
  case class BikeShareEntity(instant: String, dteday: String, season: Double, yr: Double, mnth: Double,
                             hr: Double, holiday: Double, weekday: Double, workingday: Double, weathersit: Double, temp: Double,
                             atemp: Double, hum: Double, windspeed: Double, casual: Double, registered: Double, cnt: Double)

  def main(args: Array[String]): Unit = {
    setLogger
    val doTrain = (args != null && args.length > 0 && "Y".equals(args(0)))
    val sc = new SparkContext(new SparkConf().setAppName("BikeClassificationLG").setMaster("local[*]"))

    println("============== preparing data ==================")
    val (trainDataWithMap, validateDataWithMap) = prepare(sc)
    trainDataWithMap.persist(); validateDataWithMap.persist();

    if (!doTrain) {
      println("============== train Model (Category) ==================")
      val (model2, duration2) = trainModel(trainDataWithMap, 10, 1, 1)
      val auc2 = evaluateModel(validateDataWithMap, model2)
      println("validate auc(category)=%f".format(auc2))
    } else {
      println("============== tuning parameters(Category) ==================")
      tuneParameter(trainDataWithMap, validateDataWithMap)
    }

    trainDataWithMap.unpersist(); validateDataWithMap.unpersist();
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
    val yrMap = bikeData.map { x => x.yr }.distinct().collect().zipWithIndex.toMap
    val seasonMap = bikeData.map { x => x.season }.distinct().collect().zipWithIndex.toMap
    val mnthMap = bikeData.map { x => x.mnth }.distinct().collect().zipWithIndex.toMap
    val holidayMap = bikeData.map { x => x.holiday }.distinct().collect().zipWithIndex.toMap
    val weekdayMap = bikeData.map { x => x.weekday }.distinct().collect().zipWithIndex.toMap
    val workdayMap = bikeData.map { x => x.workingday }.distinct().collect().zipWithIndex.toMap
    val weatherMap = bikeData.map { x => x.weathersit }.distinct().collect().zipWithIndex.toMap
    val hrMap = bikeData.map { x => x.hr }.distinct().collect().zipWithIndex.toMap
    //Standardize 
    val featureRddWithMap = bikeData.map { x =>
      Vectors.dense(getFeatures(x, yrMap, seasonMap, mnthMap, hrMap, holidayMap, weekdayMap, workdayMap, weatherMap))
    }
    //TODO：實作Standardize featureRddWithMap邏輯
    val stdScalerWithMap = new StandardScaler(withMean = true, withStd = true).fit(featureRddWithMap)
    //處理Category feature
    val lpData = bikeData.map { x =>
      {
        //TODO：完成產生LabeledPoint的邏輯
      }
    }
    //以6:4的比例隨機分割，將資料切分為訓練及驗證用資料
    //TODO: 實作6:4的比例隨機分割，產生trainData及 validateData
    (trainData, validateData)
  }

  /**
   * 將Category的Feature("yr","season","mnth","holiday","weekday","workingday","weathersit")轉成1-of-k encode的編碼Array
   */
  def getCategoryFeature(fieldVal: Double, categoryMap: Map[Double, Int]): Array[Double] = {
    //TODO: 實作將傳入的fieldVal及CategoryMap進行1-of-k encode邏輯
  }

  def getFeatures(bikeData: BikeShareEntity, yrMap: Map[Double, Int], seasonMap: Map[Double, Int], mnthMap: Map[Double, Int],
                  hrMap: Map[Double, Int], holidayMap: Map[Double, Int], weekdayMap: Map[Double, Int], workdayMap: Map[Double, Int],
                  weatherMap: Map[Double, Int]): Array[Double] = {
    //TODO: 完成getFeatures方法實作(提醒：類別型變數欄位要傳入getCategoryFeature進行1-of-k encode)
  }

  def trainModel(trainData: RDD[LabeledPoint],
                 numIterations: Int, stepSize: Double, miniBatchFraction: Double): (LogisticRegressionModel, Double) = {
    val startTime = new DateTime()
    //TODO: 實作訓練LogisticRegressionModel邏輯
    
    
    val endTime = new DateTime()
    val duration = new Duration(startTime, endTime)
    //MyLogger.debug(model.toPMML())
    (model, duration.getMillis)
  }

  def evaluateModel(validateData: RDD[LabeledPoint], model: LogisticRegressionModel): Double = {
    //TODO： 實作評估model邏輯
  }

  def tuneParameter(trainData: RDD[LabeledPoint], validateData: RDD[LabeledPoint]) = {
    val iterationArr: Array[Int] = Array(5, 10, 20, 60, 100)
    val stepSizeArr: Array[Double] = Array(10, 50, 100, 200)
    val miniBatchFractionArr: Array[Double] = Array(0.5, 0.8, 1)
    val evalArr =
      //TODO: 實作評估不同參數組合，並選出最佳參數組合邏輯
      
    val bestEval = (evalArr.sortBy(_._4).reverse)(0)
    println("best parameter: parameter: iteraion=%d, stepSize=%f, miniBatchFraction=%f, auc=%f"
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
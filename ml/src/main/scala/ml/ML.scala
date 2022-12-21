package ML
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.functions.rand
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.immutable.HashMap
import java.nio.file.{Paths, Files}
import scala.io.StdIn
import org.apache.spark.mllib.evaluation.RegressionMetrics

import scala.io.StdIn._

object App {

  def main(args: Array[String]): Unit = {

    //execution parameters fixed by args, some with default values.
    var savepath=""
    var modelpath=""
    var datapath=""
    var trainpath="hdfs://hadoop-master:9000/data/trainset.csv"
    var testpath="hdfs://hadoop-master:9000/data/testset.csv"
    var modelML: Int = 0
    var train = false
    var modeldict = HashMap("--lr"->1, "--rf"->2, "--gbt"->3)

    //Argument parsing  
    if(args.length<3){
      println ("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
      println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
      println("args marked with a * are optional, if they are not provided, sample data will be used ")
      println("model options are:")
      println("--lr : Linear Regression")
      println("--rf : Random Forest")
      println("--gbt : Gradient Boosted Trees")
      return
    }
    if(args(0)== "--train" ){
      train=true
    }
    else if(args(0)!="--predict"){
      println(args(0))
      println("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
      println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
      println("args marked with a * are optional, if they are not provided, sample data will be used")
      println("model options are:")
      println("--lr : Linear Regression")
      println("--rf : Random Forest")
      println("--gbt : Gradient Boosted Trees")
      return
    }
    if(train){
      args.length match{
        case 3=>
          try{
            modelML=modeldict(args(1))
            savepath=args(2)
          }catch{
            case _ : Throwable=>
              println("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
              println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
              println("args marked with a * are optional, if they are not provided, sample data will be used")
              println("model options are:")
              println("--lr : Linear Regression")
              println("--rf : Random Forest")
              println("--gbt : Gradient Boosted Trees")
              return
          }
        case 5=>
          try{
            modelML=modeldict(args(3))
            trainpath=args(1)
            testpath=args(2)
            savepath=args(4)
          }catch{
            case _ : Throwable=>
              println("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
              println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
              println("args marked with a * are optional, if they are not provided, sample data will be used")
              println("model options are:")
              println("--lr : Linear Regression")
              println("--rf : Random Forest")
              println("--gbt : Gradient Boosted Trees")
              return
          }
        case _ =>
              println("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
              println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
              println("args marked with a * are optional, if they are not provided, sample data will be used")
              println("model options are:")
              println("--lr : Linear Regression")
              println("--rf : Random Forest")
              println("--gbt : Gradient Boosted Trees")
              return
    }
    }else{
      if (args.length!=4){
              println("use run --train <path to trainset>* <path to testset>* <model> <path to save model>")
              println("Or run --predict <path to preprocessed flight data> <path to model> <path to save predictions>")
              println("args marked with a * are optional, if they are not provided, sample data will be used")
              println("model options are:")
              println("--lr : Linear Regression")
              println("--rf : Random Forest")
              println("--gbt : Gradient Boosted Trees")
              return
      }
      else{
        datapath=args(1)
        modelpath=args(2)
        savepath=args(3)
      }
    }

    
    //Spark session configuration for the app. If running over a cluster, setMaster must be "yarn" or "yarn.cluster", depending on the version
    val conf= new SparkConf()
      .setAppName("ML")
      .setMaster("local[*]")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.memoryOverHead", "2g")

    val spark= SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

  //Carry out training or prediction.
  if(train){
        val model=getmodel(spark, modelML, trainpath, testpath)
        model.write.overwrite().save(savepath)
      }
  else{
        val results=getpreds(spark, modelpath, datapath).select("Month", "DayofMonth", "DayOfWeek", "FlightNum", "DepDelay", "Distance", "TaxiOut", "Origin_t", "Dest_t", "UniqueCarrier_t", "DepHour", "DepMin", "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin", "CompoundDelay", "ArrDelay","prediction")
        results.write.mode("overwrite").option("header", "true").csv(savepath)
      }
  }



  //Takes a Dataframe that has been preprocessed.
  def getpreds(spark: SparkSession, modelpath: String, setpath: String): DataFrame={
      val model= CrossValidatorModel.load(modelpath)
      var data = spark.read.option("header", "true").csv(setpath)
      val castint = udf(( x: String)=>x.toFloat)
      for(column<-data.columns){
        data=data.withColumn(column, castint(data(column)) )
    }
      val imputeddata=handleNAValues(spark, data.na.drop(), data, data.columns)
      return model.transform(imputeddata)

  }
  //Takes a training and a test set to report metrics once done.
  def getmodel(spark: SparkSession, modelML: Int, trainpath: String, testpath: String ):CrossValidatorModel={
    
    var trainset = spark.read.option("header", "true").csv(trainpath)
    var testset = spark.read.option("header", "true").csv(testpath)
    val castint = udf(( x: String)=>x.toFloat)
    var numcolumns : List[String]= List()
    for(column<-Array("DepDelay", "Distance", "TaxiOut",	 "DepHour", "DepMin", "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin", "CompoundDelay")){
      if(trainset.columns.contains(column)){
        numcolumns=numcolumns:::List(column)
      }
    }
  for(column<-numcolumns){
      testset=testset.withColumn(column, testset(column).cast("float") )
    }

     for(column<-numcolumns){
      trainset=trainset.withColumn(column, trainset(column).cast("float") )
        }

    trainset=handleNAValues(spark,trainset.na.drop(), trainset, (trainset.columns.toSet -- Array("ArrDelay").toSet).toArray)
    testset=handleNAValues(spark, testset.na.drop(), testset, (testset.columns.toSet -- Array("ArrDelay").toSet).toArray)

    for(column<-trainset.columns){
      testset=testset.withColumn(column, testset(column).cast("double") ).na.drop()
    }

    for(column<-trainset.columns){
      trainset=trainset.withColumn(column, trainset(column).cast("double") ).na.drop()
      }
    
 
    val featuresinp= (trainset.columns.toSet -- Array("ArrDelay").toSet).toArray
    //Creates the variable vectors used in the algorithms
    val assembler = new VectorAssembler()
      .setInputCols(featuresinp)
      .setOutputCol("features")
      .setHandleInvalid("keep")
    //normalizes the feature vectors. The reason this is not in the preprocessing step is to avoid the highest values in the
    //training set influencing the test set.
    

    val featuresNormalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    modelML match {
      case 1 => 
          println("Training linear regression model...")
          val lr = new LinearRegression()
          .setLabelCol("ArrDelay")
          .setFeaturesCol("normFeatures")

  // We use a ParamGridBuilder to construct a grid of parameters to search over.
      val paramGrid = new ParamGridBuilder()
            .addGrid(lr.regParam, Array(0.1, 0.01))
            .addGrid(lr.fitIntercept)
            .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
            .build()

  //Create the pipeline, containig the assembler, the feature normalizer, and, finally, the linear regression model.
      val pipeline = new Pipeline().setStages(Array(assembler, featuresNormalizer, lr))

  // Make predictions on test data. model is the model with combination of parameters
  // that performed best.
      
      val cv = new CrossValidator()
        .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol("ArrDelay"))
        .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

      trainset.select("ArrDelay").show()
      val model =cv.fit(trainset)
  
      val transf_test = model.transform(testset).select("ArrDelay", "prediction")

    //we create a regression metrics object by passing it a vector of tuples containing the prediction vs the actual value
    val rm = new RegressionMetrics(transf_test.rdd.map(x =>
          (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
      
      val lrm: LinearRegressionModel = model
      .bestModel.asInstanceOf[PipelineModel]
      .stages
      .last.asInstanceOf[LinearRegressionModel]
      println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
      println("MAE: " + 	rm.meanAbsoluteError)
      println("R Squared: " + rm.r2)                         
      println("Explained Variance: " + rm.explainedVariance + "\n")
      println("Linear Regression coefficients: "+lrm.coefficients+ "\n")
      println("Linear Regression intercept: "+ lrm.intercept+"\n")
      return model
      

    case 2 =>

      //Same conditions apply for both below models

      val rf = new RandomForestRegressor()
        .setLabelCol("ArrDelay")
        .setFeaturesCol("features")

      val pipeline = new Pipeline().setStages(Array(assembler, rf))

      val paramGrid = new ParamGridBuilder() 
        .addGrid(rf.numTrees, Array(5, 20, 50)) 
        .addGrid(rf.maxDepth,  Array(2, 5, 10)) 
        .addGrid(rf.maxBins, Array(5, 10, 20)) 
        .build() 

      
      val cv = new CrossValidator() 
        .setEstimator(pipeline) 
        .setEvaluator(new RegressionEvaluator().setLabelCol("ArrDelay")) 
        .setEstimatorParamMaps(paramGrid) 
        .setNumFolds(2)  

      println("Training model with Random Forest algorithm")  
      val model = cv.fit(trainset) 

      val transf_test = model.transform(testset).select("ArrDelay", "prediction")
      val rm = new RegressionMetrics(transf_test.rdd.map(x =>
            (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

          println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
          println("MAE: " + 	rm.meanAbsoluteError)
          println("R Squared: " + rm.r2)                         
          println("Explained Variance: " + rm.explainedVariance + "\n")
          return model

      
      case _ =>
          val gbt = new GBTRegressor()
            .setLabelCol("ArrDelay")
            .setFeaturesCol("normFeatures")
            .setMaxIter(10)

          val pipeline = new Pipeline().setStages(Array(assembler, featuresNormalizer, gbt))

          //no parameters to search over, empty paramGrid
          val paramGrid = new ParamGridBuilder().build() 

          val cv = new CrossValidator() 
          .setEstimator(pipeline) 
          .setEvaluator(new RegressionEvaluator().setLabelCol("ArrDelay"))
          .setEstimatorParamMaps(paramGrid)  
          .setNumFolds(2)  
          

          println("Training GBT Model...")
          val model = cv.fit(trainset)

          val transf_test = model.transform(testset).select("ArrDelay", "prediction")

          val rm = new RegressionMetrics(transf_test.rdd.map(x =>
            (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

          println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
          println("MAE: " + 	rm.meanAbsoluteError)
          println("R Squared: " + rm.r2)                         
          println("Explained Variance: " + rm.explainedVariance + "\n")
          return model
         
        } 
    }

    def handleNAValues(spark: SparkSession, dataset_sinnull: DataFrame,dataset_connull: DataFrame, columnsToProcess: Array[String]): DataFrame = {
    //Categorical columns= StringType, numerical columns = FloatType
    dataset_sinnull.createOrReplaceTempView("table")
    var dataset_connull2=dataset_connull

        columnsToProcess.foreach(column => {
          var columnType = "StringType"
          dataset_sinnull.dtypes.foreach(tuple => {
            if (tuple._1 == column) {
              columnType = tuple._2
            }
          })



          println(s"imputing Column ${column} with type ${columnType}")
          columnType match {
            case "FloatType" => {
              val columnMean =dataset_sinnull.agg(avg(column)).first().getDouble(0)
    //first devuelve la primera linea del dataframe as a Row. getDouble(i)	returns the value at position i as a primitive double.
              dataset_connull2 = dataset_connull.na.fill(columnMean, Array(column))
            }
            case "StringType" => {
    //Approx_cound_distinct returns long type value, therefore i use 'getLong'
    //val mode_value = dataset_sinnull.agg(approx_count_distinct(column)).first().getLong(0)
    val a= "SELECT "
    val b=", COUNT("
    val c=") AS countss FROM table GROUP BY "
    val d= " ORDER BY countss desc"
    val mode_value=spark.sql(a+column+b+column+c+column+d).first.getString(0)
    dataset_connull2 = dataset_connull.na.fill("mode_value", Seq(column))
            }
          }
        })
   return  dataset_connull2
  }
}

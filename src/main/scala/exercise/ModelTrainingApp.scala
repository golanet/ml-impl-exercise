package exercise

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by TG on 02/03/2018.
  */
object ModelTrainingApp {

  def train(historicalData: DataFrame): Unit = {

    val maxNullValuesPercentile = 0.7
    val trainSplit: Double = 0.95
    val labelCol = "response"
    val featuresVectorCol = "features"
    val predictedLabelCol = "predictedLabel"

    // Input Schema
    // I manually did exploration on the given dataset to understand each feature (distinct values and counts for each value)
    // Spark mllib also have a VectorIndexer Estimator&Model to auto-identify categorical features by maxCategories
    //
    // f17 does not exist
    // f19 looks not useful - unique textual values
    // I manually chose the categorical features according to maxCategories = 8
    val rawFeatureCols: FeatureColumns = new FeatureColumns()
      .setCategoricalCols(
        "feature_1",
        "feature_3",
        "feature_7",
        "feature_12",
        "feature_13",
        "feature_15",
        "feature_22",
        "feature_24",
        "feature_26",
        "feature_27",
        "feature_28",
        "feature_30")
      .setTextualCols("feature_14")
      .setNumericCols(
        "feature_2",
        "feature_4",
        "feature_5",
        "feature_6",
        "feature_8",
        "feature_9",
        "feature_10",
        "feature_11",
        "feature_16",
        "feature_18",
        "feature_20",
        "feature_21",
        "feature_23",
        "feature_25",
        "feature_29")

    debugDataset(historicalData)("loaded historical data")

    // pre-processing
    // prepare the historical data
    val instancesCount = historicalData.count()
    println(s"Total instances count is $instancesCount")

    // drop features which have high volume of null-values
    val (filteredRawFeatureCols: FeatureColumns, cleanNullData: DataFrame) = {
      import sql.functions._

      val rawFeatureCounts: Row = historicalData
        .describe(rawFeatureCols.allCols: _*)
        .where(col("summary") === "count")
        .select(rawFeatureCols.allCols.map(col(_)): _*)
        .first()

      val rawFeatureColsToDrop: Array[String] = for {
        rawFeatureCol <- rawFeatureCols.allCols
        actualNonNullCount = rawFeatureCounts.getAs[String](rawFeatureCol).toLong
        actualNullCount = instancesCount - actualNonNullCount
        if actualNullCount > instancesCount * maxNullValuesPercentile
      } yield rawFeatureCol

      println(
        s"""
           |Filtering out the following features duo to high null-values volume:
           |${rawFeatureColsToDrop.mkString("\t", "\n\t", "")}
        """.stripMargin)

      // return the actual raw feature cols we are going to use
      val _actualFeatureCols: FeatureColumns = rawFeatureCols.drop(rawFeatureColsToDrop: _*)
      (_actualFeatureCols, historicalData.drop(rawFeatureColsToDrop.toSeq: _*))
    }

    debugDataset(cleanNullData)("described historical data")

    // fill missing values for numeric features with avg/mid
    // First, cast numeric fields from int to double
    val withNumericAsDouble = numericAsDouble(cleanNullData)(filteredRawFeatureCols.getNumericCols)
    val imputer = new Imputer()
      .setInputCols(filteredRawFeatureCols.getNumericCols)
      .setOutputCols(filteredRawFeatureCols.getNumericCols)
      .setStrategy("mean") // TODO: should decide
    val imputerModel: ImputerModel = imputer.fit(withNumericAsDouble)

    val preparedHistoricalData = imputerModel.transform(withNumericAsDouble)
    debugDataset(preparedHistoricalData)("prepared historical data")

    // Feature engineering
    // Categorical features
    // - Handle all features in the same approach
    val (extractedCategoricalFeatureCols: Array[String], categoricalFeatureExtraction: Array[Pipeline]) = filteredRawFeatureCols
      .getCategoricalCols.map { categoricalFeatureCol =>
      val outputCol = s"extracted_cat_$categoricalFeatureCol"
      (outputCol, getOneHotEncoderPipeline(inputCol = categoricalFeatureCol, outputCol = outputCol))
    }.unzip

    // Textual features
    // - specific handling for each feature
    val (extractedTextualFeatureCols: Array[String], textualFeatureExtraction: Array[Pipeline]) = {
      val _textualFeatureCols = mutable.ArrayBuffer[String]()
      val _textualFeatureExtraction = mutable.ArrayBuffer[Pipeline]()

      // f14
      val f14TfIdfExtraction: Unit = {
        val theRawFeatureCol = "feature_14"
        if (filteredRawFeatureCols.contains(theRawFeatureCol)) {
          val outputCol = s"extracted_textual_$theRawFeatureCol"
          _textualFeatureCols.append(outputCol)
          _textualFeatureExtraction.append(getTFIDFPipeline(inputCol = theRawFeatureCol, outputCol))
        }
      }

      (_textualFeatureCols.toArray, _textualFeatureExtraction.toArray)
    }

    // update the current feature cols after we are going to use for the model with the extracted features
    val extractedFeatureCols: FeatureColumns = filteredRawFeatureCols
      .setCategoricalCols(extractedCategoricalFeatureCols: _*)
      .setTextualCols(extractedTextualFeatureCols: _*)

    println(
      s"""
         |The following features are attending in the training:
         |${extractedFeatureCols.allCols.mkString("\t", "\n\t", "")}
         |""".stripMargin)

    // Assemble all features
    val va = new VectorAssembler()
      .setInputCols(extractedFeatureCols.allCols)
      .setOutputCol(featuresVectorCol)

    // Fit the feature extraction pipeline which takes care of creating the feature vector column
    // We would like to fit on the entire data to see all possible values and not only the training set
    val featureExtractionPipelineModel: PipelineModel = new Pipeline()
      .setStages(textualFeatureExtraction ++ categoricalFeatureExtraction ++ Array(va))
      .fit(preparedHistoricalData)

    // Index labels, adding metadata to the label column.
    // We skipped the Estimator here as we know what binary values are - true/false
    val binaryLabelIndexer = new StringIndexerModel(labels = Array("no", "yes"))
      .setInputCol(labelCol)
      .setOutputCol(s"${labelCol}_indexed")
      .setHandleInvalid("skip")

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol(binaryLabelIndexer.getOutputCol)
      .setFeaturesCol(featuresVectorCol)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol(rf.getPredictionCol)
      .setOutputCol(predictedLabelCol)
      .setLabels(binaryLabelIndexer.labels)

    // Chain f.extraction and label indexer and r.forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureExtractionPipelineModel, binaryLabelIndexer, rf, labelConverter))

    // Choose best model
    // Model tuning - the set of parameters
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(4, 6, 8))
      .addGrid(rf.numTrees, Array(10, 30, 50))
      .build()

    val binaryClassificationEvaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol(rf.getLabelCol)
      .setRawPredictionCol(rf.getRawPredictionCol)

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(binaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // Split the data into training and test sets
    val Array(trainingSet, testSet) = preparedHistoricalData.randomSplit(Array(trainSplit, 1 - trainSplit))

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(trainingSet)

    // Evaluate the model metric using the test set
    val testSetAreaUnderROC: Double = binaryClassificationEvaluator
      .evaluate(debugDataset(cvModel.transform(testSet))("cv model predictions on test set"))

    println(s"AreaUnderROC for test set is $testSetAreaUnderROC")
  }

  private def getTFIDFPipeline(inputCol: String, outputCol: String): Pipeline = {
    val tokenizer = new Tokenizer()
      .setInputCol(inputCol)
      .setOutputCol(s"_words_$inputCol")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol(s"_rawFeatures_$inputCol")
      .setNumFeatures(20) // TODO: should be defined

    val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol(outputCol)

    new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
  }

  private def getOneHotEncoderPipeline(inputCol: String, outputCol: String): Pipeline = {
    val indexer = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(s"_categoryIndex_$inputCol")
      .setHandleInvalid("keep") // we will keep unseen values

    val encoder = new OneHotEncoder()
      .setInputCol(indexer.getOutputCol)
      .setOutputCol(outputCol)

    new Pipeline().setStages(Array(indexer, encoder))
  }

  private def numericAsDouble(cleanNullData: DataFrame)(numericFeatureCols: Seq[String]) = {
    import functions._
    numericFeatureCols.foldLeft(cleanNullData) { (prevDF, numericCol) =>
      prevDF.withColumn(numericCol, col(numericCol).cast(DoubleType))
    }
  }

  private def debugDataset(ds: Dataset[_])(name: String): Dataset[_] = {
    ds.cache()
    println(s"Debug Dataset $name")
    println(s"Dataset count #${ds.count()}")
    ds.show(truncate = false)
    ds.printSchema()
    ds
  }
}

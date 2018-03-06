package exercise

/**
  * Created by TG on 03/03/2018.
  * Immutable Structure for representing the feature cols and their type
  */
case class FeatureColumns(private val colToType: Map[String, String] = Map.empty) {
  private val TEXTUAL = "textual"
  private val NUMERIC = "numeric"
  private val CATEGORICAL = "categorical"
  private val groupedByType: Map[String, Array[String]] = colToType.groupBy { case (col, t) => t }.mapValues(_.keys.toArray)

  lazy val allCols: Array[String] = colToType.keys.toArray

  def getTextualCols: Array[String] = groupedByType.getOrElse(TEXTUAL, Array.empty)
  def getNumericCols: Array[String] = groupedByType.getOrElse(NUMERIC, Array.empty)
  def getCategoricalCols: Array[String] = groupedByType.getOrElse(CATEGORICAL, Array.empty)

  def setTextualCols(cols: String*): FeatureColumns = {
    this.copy(this.colToType -- getTextualCols ++ cols.map((_,TEXTUAL)))
  }

  def setNumericCols(cols: String*): FeatureColumns = {
    this.copy(this.colToType -- getNumericCols ++ cols.map((_,NUMERIC)))
  }

  def setCategoricalCols(cols: String*): FeatureColumns = {
    this.copy(this.colToType -- getCategoricalCols ++ cols.map((_,CATEGORICAL)))
  }

  def drop(droppedCols: String*): FeatureColumns = {
    this.copy(this.colToType -- droppedCols)
  }

  def contains(col: String): Boolean = this.colToType.contains(col)
}

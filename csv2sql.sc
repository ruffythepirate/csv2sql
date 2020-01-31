#!env amm
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.text.SimpleDateFormat


@doc("Takes an input as piped data and outputs it as input statements in sql.")
@main
def main(
          table: String @doc("Name of the table that the insert statements should be inserting data to."),
          separator: String @doc("Separator character for the csv.") = ","
        ) = {

  val inputIterator = createInputIterator(System.in)
  val columnDefinitions = findColumnDefinitions(inputIterator.next())

  inputIterator
    .takeWhile(_ != null)
    .map( generateInsertStatement(table, _, columnDefinitions))
    .foreach(System.out.println(_))

}

case class ColumnDefinition(name: String, valueConverter: String => String)

def createInputIterator(in: InputStream) = {
  val inputStreamReader = new InputStreamReader(in)
  val bufferedReader = new BufferedReader(inputStreamReader)

  val lineIterator = Iterator
    .continually(bufferedReader.readLine)

  if(!lineIterator.hasNext) {
    throw new RuntimeException("Trying to start a input stream listener, but there is no data coming in. Please pipe data to this tool to use it!")
  }
  lineIterator
}


def findColumnDefinitions(columnRow: String) = {
  columnRow.split(",")
    .map(_.trim)
    .map(column => {
    column.split(" ") match {
      case Array(name: String) =>
        ColumnDefinition(name, findConvertMethod("default"))
      case Array(name: String, dataType: String) =>
        ColumnDefinition(name, findConvertMethod(dataType))
    }
  })
}

val dateWithPattern = """date\((.*)\)""".r

object DateUtils {
  def convertDateStringToSqlDateString(s: String, format: String) = {
    val dateFormat = new SimpleDateFormat(format)
    val sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    sqlDateFormat.format(dateFormat.parse(s))
  }
}

def findConvertMethod(dataType: String) = {
  dataType.toLowerCase() match {
    case dateWithPattern(pattern) =>
      fieldValue: String => s"'${DateUtils.convertDateStringToSqlDateString(fieldValue, pattern)}'"
    case "date" =>
      fieldValue: String => s"'$fieldValue'"
    case "varchar" =>
      fieldValue: String => s"'$fieldValue'"
    case "float" =>
      fieldValue: String => fieldValue
    case _ =>
      fieldValue: String => fieldValue
  }
}

def generateInsertStatement(table: String, rowToConvert: String, columnDefinitions: Seq[ColumnDefinition]) = {

  val columnNames = columnDefinitions.map(_.name).mkString(",")

    val valueStatement = rowToConvert
      .split(",")
      .zip(columnDefinitions)
      .map{case(columnValue: String, columnDefinition: ColumnDefinition) =>
        columnDefinition.valueConverter(columnValue)
      }
      .mkString(",")

  s"INSERT INTO $table ($columnNames) VALUES ($valueStatement)"
}

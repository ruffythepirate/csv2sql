#! /usr/local/bin/amm
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Locale


@doc("Takes an input as piped data and outputs it as input statements in sql.")
@main
def main(
          table: String @doc("Name of the table that the insert statements should be inserting data to."),
          database: String @doc("Name of the database where the table resides in. A use statement for this database will be generated on top."),
          separator: String @doc("Separator character for the csv.") = ",",
          generateCreateDb: Boolean @doc("Decides if a create statement for the db should be generated") = false,
          generateCreateTable: Boolean @doc("Decides if a create statement for the table should be generated") = false,
          localeCode: String @doc("The locale to use for number formatting when reading the csv (output is US)") = "US"
        ) = {

  val locale = Locale.getAvailableLocales.find(l => l.getCountry == localeCode).get
  val inputIterator = createInputIterator(System.in)
  val columnDefinitions = findColumnDefinitions(inputIterator.next(), separator, locale)



  if(generateCreateDb) {
    outputCreateDb(database)
  }
  System.out.println(s"use $database;")

  if(generateCreateTable) {
    outputCreateTable(table, columnDefinitions)
  }
  inputIterator
    .takeWhile(_ != null)
    .map( generateInsertStatement(separator, table, _, columnDefinitions))
    .foreach(System.out.println(_))

}

case class ColumnDefinition(name: String, columnType: String, valueConverter: String => String)

def outputCreateDb(dbName: String) = {
  System.out.println(s"CREATE DATABASE IF NOT EXISTS $dbName;")
}

def outputCreateTable(tableName: String, columnDefinitions: Seq[ColumnDefinition]) = {
  System.out.println(s"CREATE TABLE IF NOT EXISTS $tableName (${columnDefinitions.map(d => s"${d.name} ${d.columnType}").mkString(",")});")
}

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

def findColumnType(dataType: String): String = {
  dataType.toLowerCase() match {
    case dateWithPattern(pattern) =>
      "date"
    case "varchar" =>
      "varchar(255)"
    case columnType: String =>
      columnType
  }
}


object DateUtils {
  def convertDateStringToSqlDateString(s: String, format: String) = {
    val dateFormat = new SimpleDateFormat(format)
    val sqlDateFormat = new SimpleDateFormat("yyyy-mm-dd")
    sqlDateFormat.format(dateFormat.parse(s))
  }
}

def findConvertMethod(dataType: String, locale: Locale) = {
  dataType.toLowerCase() match {
    case dateWithPattern(pattern) =>
      fieldValue: String => s"'${DateUtils.convertDateStringToSqlDateString(fieldValue, pattern)}'"
    case "date" =>
      fieldValue: String => s"'$fieldValue'"
    case "varchar" =>
      fieldValue: String => s"'${fieldValue.replace("'", "\\'")}'"
    case "float" =>
      fieldValue: String => val num = NumberFormat.getInstance(locale).parse(fieldValue)
      num.floatValue().toString
    case _ =>
      fieldValue: String => fieldValue
  }
}

def findColumnDefinitions(columnRow: String, separator: String, locale: Locale) = {
  columnRow.split(separator)
    .map(_.trim)
    .map(column => {
    column.split(" ") match {
      case Array(name: String) =>
        ColumnDefinition(name, "VARCHAR(256)", findConvertMethod("default", locale))
      case Array(name: String, dataType: String) =>
        ColumnDefinition(name, findColumnType(dataType), findConvertMethod(dataType, locale))
    }
  })
}

val dateWithPattern = """date\((.*)\)""".r


def generateInsertStatement(separator:String, table: String, rowToConvert: String, columnDefinitions: Seq[ColumnDefinition]) = {

  val columnNames = columnDefinitions.map(_.name).mkString(",")

    val valueStatement = rowToConvert
      .split(separator)
      .zip(columnDefinitions)
      .map{case(columnValue: String, columnDefinition: ColumnDefinition) =>
        columnDefinition.valueConverter(columnValue)
      }
      .mkString(",")

  s"INSERT INTO $table ($columnNames) VALUES ($valueStatement);"
}

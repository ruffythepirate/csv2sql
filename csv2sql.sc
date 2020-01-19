#!env amm

@doc("Takes an input as piped data and outputs it as input statements in sql.")
@main
def main(
          table: String @doc("Name of the table that the insert statements should be inserting data to."),
          separator: String @doc("Separator character for the csv.) = ","
        ) = {

  val columnDefinitions = findColumnDefinitions("hello varchar,world float")

  System.out.println(generateInsertStatement(table, "black, 1.2", columnDefinitions))

}

case class ColumnDefinition(name: String, valueConverter: String => String)


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

def findConvertMethod(dataType: String) = {
  dataType.toLowerCase() match {
    case "varchar" =>
      fieldValue: String => s"'$fieldValue''"
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

/**
 * Hakki Buyukkahraman
 * 06.02.2021
 * v1.0
 *
 * "Answers" object reads a csv file with columns "date, productId, eventName, userId"
 * and find answers to following questions.
 * 1- Unique Product View counts by ProductId
 * 2- Unique Event counts
 * 3- Top 5 Users who fulfilled all the events (view, add, remove, click)
 * 4- All events of #UserId: 47
 * 5- Product Views of #UserId: 47
 *
 * Notes:
 * 1. "connect" object creates 8 files by default. When defining the number of files as 1, no files are created.
 *    Hence, number of files is set to 2 for now. Issue is still under investigation.
 *
 */

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.table.sources.CsvTableSource

object Answers {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val csvTable = CsvTableSource
      .builder
      .path("file:/C:/case.csv")
      .ignoreFirstLine
      .fieldDelimiter("|")
      .field("date", INT)
      .field("productId", INT)
      .field("eventName", STRING)
      .field("userId", INT)
      .build

    val tmpTable = tableEnv.fromTableSource(csvTable)

    tableEnv.createTemporaryView("Activities", tmpTable)

    // Question 1: Unique Product View counts by ProductId
    val result1 = tableEnv.sqlQuery("SELECT productId, count(distinct userId) " +
      "FROM Activities " +
      "WHERE eventName = 'view' " +
      "GROUP BY productId")

    val schema1 = new Schema()
      .field("a", INT)
      .field("b", BIGINT)

    tableEnv.connect(new FileSystem().path("file:/C:/result1"))
      .withFormat(new OldCsv()
        .fieldDelimiter("|")
        .numFiles(2) // when numFiles = 1, no output?
        .writeMode("OVERWRITE")
      )
      .withSchema(schema1)
      .createTemporaryTable("CsvSinkTable1")

    result1.executeInsert("CsvSinkTable1")

    // Question 2: Unique Event counts
    val result2 = tableEnv.sqlQuery("SELECT eventName, count(*) " +
      "FROM Activities " +
      "GROUP BY eventName")

    val schema2 = new Schema()
      .field("a", STRING)
      .field("b", BIGINT)

    tableEnv.connect(new FileSystem().path("file:/C:/result2"))
      .withFormat(new OldCsv()
        .fieldDelimiter("|")
        .numFiles(2) // when numFiles = 1, no output?
        .writeMode("OVERWRITE")
      )
      .withSchema(schema2)
      .createTemporaryTable("CsvSinkTable2")

    result2.executeInsert("CsvSinkTable2")

    // Question 3: Top 5 Users who fulfilled all the events (view, add, remove, click)
    val result3 = tableEnv.sqlQuery("SELECT userId " +
      "FROM (" +
      " SELECT userId, count(distinct eventName) cntEvent, count(*) cnt" +
      " FROM Activities " +
      " GROUP BY userId" +
      ")" +
      "WHERE cntEvent = 4" +
      "ORDER BY cnt DESC " +
      "FETCH FIRST 5 ROWS ONLY")

    // Since previous sqlQuery also returns the column cnt, although it is not in select, we are omitting it with a second query
    val result3_1 = tableEnv.sqlQuery("SELECT userId FROM " + result3)

    val schema3 = new Schema()
      .field("a", INT)

    tableEnv.connect(new FileSystem().path("file:/C:/result3"))
      .withFormat(new OldCsv()
        .fieldDelimiter("|")
        .numFiles(2) // when numFiles = 1, no output?
        .writeMode("OVERWRITE")
      )
      .withSchema(schema3)
      .createTemporaryTable("CsvSinkTable3")

    result3_1.executeInsert("CsvSinkTable3")

    // Question 4: All events of #UserId: 47
    val result4 = tableEnv.sqlQuery("SELECT eventName, count(*) " +
      "FROM Activities " +
      "WHERE userId = 47 " +
      "GROUP BY eventName")

    val schema4 = new Schema()
      .field("a", STRING)
      .field("b", BIGINT)

    tableEnv.connect(new FileSystem().path("file:/C:/result4"))
      .withFormat(new OldCsv()
        .fieldDelimiter("|")
        .numFiles(2) // when numFiles = 1, no output?
        .writeMode("OVERWRITE")
      )
      .withSchema(schema4)
      .createTemporaryTable("CsvSinkTable4")

    result4.executeInsert("CsvSinkTable4")

    // Question 5: Product Views of #UserId: 47
    val result5 = tableEnv.sqlQuery("SELECT distinct productId " +
      "FROM Activities " +
      "WHERE userId = 47 " +
      "  AND eventName = 'view'")

    val schema5 = new Schema()
      .field("a", INT)

    tableEnv.connect(new FileSystem().path("file:/C:/result5"))
      .withFormat(new OldCsv()
        .fieldDelimiter("|")
        .numFiles(2) // when numFiles = 1, no output?
        .writeMode("OVERWRITE")
      )
      .withSchema(schema5)
      .createTemporaryTable("CsvSinkTable5")

    result5.executeInsert("CsvSinkTable5")

  }
}

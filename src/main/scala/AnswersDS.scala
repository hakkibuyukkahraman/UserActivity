/**
 * Hakki Buyukkahraman
 * v1.0 - 06.02.2021 - Initial version
 * v1.1 - 28.02.2021 - DataStream API used instead of Table API
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
 * 2. "writeAsCsv" and "writeAsText" creates 8 files by default. When defining the parallelism as 1, "Access denied" error occurs.
 *    Hence, parallelism is set to 2 for now. Issue is still under investigation.
 */

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem

object AnswersDS {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val csvInput = env.readCsvFile[(Int, Int, String, Int)](
      "file:/C:/case.csv",
      "\n",
      "|",
      null,
      true
    )

    /** Question 1: Unique Product View counts by ProductId */
    val csvResult1 = csvInput
      .filter(x => x._3.equals("view"))
      .distinct(1, 3)
      .map(x => (x._2, 1))
      .groupBy(0)
      .sum(1)

    csvResult1.writeAsCsv(
      "file:/C:/result1",
      "\n",
      "|",
      FileSystem.WriteMode.OVERWRITE
    )
      .setParallelism(2)

    /** Question 2: Unique Event counts */
    val csvResult2 = csvInput
      .map(x => (x._3, 1))
      .groupBy(0)
      .sum(1)

    csvResult2.writeAsCsv(
      "file:/C:/result2",
      "\n",
      "|",
      FileSystem.WriteMode.OVERWRITE
    )
      .setParallelism(2)

    /** Question 3: Top 5 Users who fulfilled all the events (view, add, remove, click) */
    val csvResult3_1 = csvInput /** Event counts by Users */
      .map(x => (x._4, 1))
      .groupBy(0)
      .sum(1)

    val csvResult3_2 = csvInput /** Users who fulfilled all the events */
      .distinct(2, 3)
      .map(x => (x._4, 1))
      .groupBy(0)
      .sum(1)
      .filter(x => x._2.equals(4))

    val csvResult3 = csvResult3_1.join(csvResult3_2).where(0).equalTo(0)
      .map(x => (x._1._1, x._1._2))
      .sortPartition(1, Order.DESCENDING) /** Sort Users according to the number of events in descending order */
      .setParallelism(1) /** Apply sorting globally on the whole set */
      .first(5) /** Pick Top 5 Users */
      .map(x => x._1)

    csvResult3.writeAsText( /** Since the output has only one column, Text is used instead of CSV */
      "file:/C:/result3",
      FileSystem.WriteMode.OVERWRITE
    )
      .setParallelism(2)

    /** Question 4: All events of #UserId: 47 */
    val csvResult4 = csvInput
      .filter(x => x._4.equals(47))
      .map(x => (x._3, 1))
      .groupBy(0)
      .sum(1)

    csvResult4.writeAsCsv(
      "file:/C:/result4",
      "\n",
      "|",
      FileSystem.WriteMode.OVERWRITE
    )
      .setParallelism(2)

    /** Question 5: Product Views of #UserId: 47 */
    val csvResult5 = csvInput
      .filter(x => x._4.equals(47) && x._3.equals("view"))
      .distinct(1)
      .map(x => x._2)

    csvResult5.writeAsText( /** Since the output has only one column, Text is used instead of CSV */
      "file:/C:/result5",
      FileSystem.WriteMode.OVERWRITE
    )
      .setParallelism(2)

    env.execute()

  }

}

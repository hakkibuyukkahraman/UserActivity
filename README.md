User Activity Summary

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

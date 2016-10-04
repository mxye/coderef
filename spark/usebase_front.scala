// get 6 days worth of data

val daysToCollect = 6

val dDates =  new dataDates(new DateTime(), daysToCollect)

val paths = dDates.pathSegments.map(s => "/mnt/searchFront/"+s+"/*")

println(paths)

val fields = List("data.Params.eventType", "ts","data.Params.size", "data.Params.name as query", "data.Params.id", "data.Params.`name-with-dash`")

val filterTerm = "size != 5"
val queryFilterTerm = "eventType = 'someevent' or eventType is null"



val data = new logsByDate(paths).jsondata



data.printSchema



val frontLogs = new logsByFieldFront(data,fields,filterTerm)


frontLogs.ipLogs.show()



frontLogs.ipLogs.count



println(frontLogs.nRecords)
frontLogs.ipLogs.printSchema
println(frontLogs.allFields)
frontLogs.ipLogs.show(50)



val qlogs = new queryLogs(frontLogs.ipLogs, queryFilterTerm, "id", "query", "querySeqNumber")
qlogs.inputLogs.show(5)



frontLogs.ipLogs.createOrReplaceTempView("logs")



qlogs.queryCounts.show(100)


qlogs.queryCounts.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(s"/mnt/$writeMountName" + s"/outputs/tables/queryCounts/$currentYear$currentMonth$currentDay$currentHour")



qlogs.minQueries.show



qlogs.minQueryStrings.select("queryList").coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(s"/mnt/$writeMountName" + s"/outputs/tables/MinFrontendQueries/$currentYear$currentMonth$currentDay$currentHour")

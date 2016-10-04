
// MAGIC 
// %run "/Users/username/base"



// get 6 days worth of data


val daysToCollect = 6
//get today and before, adding up to be 2 days
val dDates =  new dataDates(new DateTime(), daysToCollect)

val paths = dDates.pathSegments.map(s => "/mnt/searchBack/"+s+"/*")

println(paths)

// val paths = List("/mnt/searchFront/2016/08/1*/*")
val backfields = List("Timestamp", "Events.Name", "Events.EventData.date", "Events.EventData.SessionId",  "Events.EventData.id")

def remove(ele: String, list: List[String]) = list diff List(ele)
val expFields = remove("Timestamp", backfields.map(f => f.substring(f.lastIndexOf(".") + 1)))


val filterTermBack = ""




val backdata = new logsByDate(paths).jsondata
val backLogs = new logsByFieldBack(backdata, backfields, filterTermBack, expFields)
backLogs.expLogs.show



val temp = backLogs.expLogs.filter("Name != 'someName' and SessionId is not null and TrackingId is not null and PathAndQuerySTring is not null")
temp.show



val pattern: scala.util.matching.Regex = """offset=(\d+)""".r


val backBits = temp.withColumn("pagination", getPagination(pattern)(temp.col("PathAndQueryString")))


backBits.show(50)
backBits.createOrReplaceTempView("backBits")


val backfilter = "searchQuery is not null and searchQuery != '' and pagination = 1 and specialty is not null"


val qlogsBack = new queryLogs(backBits, backfilter, "searchQueryGuid", "searchQuery", "Timestamp")


qlogsBack.filteredLogs.show


val topqs = qlogsBack.topqs
topqs.show(100)


topqs.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(s"/mnt/$writeMountName" + s"/outputs/tables/backendQueries/$currentYear$currentMonth$currentDay$currentHour")


val sessionQs = new queryLogs(backBits, backfilter, "sessionId", "searchQuery", "Timestamp")


qlogsBack.minQueries.show


val sessionMinqs = sessionQs.minQueryStrings



sessionMinqs.cache
sessionMinqs.show


sessionMinqs.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(s"/mnt/$writeMountName" + s"/outputs/tables/backendQueriesBySession/$currentYear$currentMonth$currentDay$currentHour")





//long to datetime
val mytime = new org.joda.time.DateTime(1469652442000L)

//datetime constructor and datetime to long
import org.joda.time.DateTime
//constructor needs to have at least the minute number, can have miliseconds 
val starttime = new DateTime(2016,10,3,18,23).getMillis()
val endtime = new DateTime(2016,10,3,19,0,59,345).getMillis()


val cleanlogs = sqlContext.sql("select * from logs where ts > 1475517600000 and ts < 1475521200000 and eventType ='autocomplete'")
cleanlogs.cache()
cleanlogs.show()

import org.joda.time.{Period, DateTime, Days, format => dtFormat}

// udf of datetime long to string, the formatting using forPattern by defining val fmt somehow is not serializable and fails,
//so I just use toString

val toDateTime = udf[String, String]( number => new org.joda.time.DateTime(number.toLong).toString)

// "23235335".toLong


// val fmt = dtFormat.DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")
// val dtStr = fmt.print(new DateTime)


val tstable = cleanlogs.withColumn("tsString",toDateTime($"ts"))
tstable.show(false)


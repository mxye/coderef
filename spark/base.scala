import java.util.Calendar
import java.text.SimpleDateFormat
val today = Calendar.getInstance().getTime()
// output:
// today: java.util.Date = Tue Aug 23 22:09:03 UTC 2016

    
// create the date/time formatters
val yearFormat = new SimpleDateFormat("yyyy")
val monthFormat = new SimpleDateFormat("MM")
val dayFormat = new SimpleDateFormat("dd")
val minuteFormat = new SimpleDateFormat("mm")
val hourFormat = new SimpleDateFormat("hh")
val amPmFormat = new SimpleDateFormat("a")

val currentHour = hourFormat.format(today)     
val currentYear = yearFormat.format(today)
val currentMonth = monthFormat.format(today)
val currentDay = dayFormat.format(today)
val amOrPm = amPmFormat.format(today) 

// output:
// currentHour: String = 06
// currentYear: String = 2016
// currentMonth: String = 10
// currentDay: String = 04
// amOrPm: String = PM


import org.joda.time.{Period, DateTime, Days, format => dtFormat}

// define formatter
val fmt = dtFormat.DateTimeFormat.forPattern("yyyy/MM/dd")

case class dataDates (end: org.joda.time.DateTime, daysToCollect: Int) {
  val start = end.minusDays(daysToCollect)
  val steps = (0 to daysToCollect-1 by 1).toList
val collectDates = steps.map(end.minusDays(_))
  val pathSegments = collectDates.map(date => fmt.print(date))
//   val countDays = Days.daysBetween(start.withTimeAtStartOfDay() , end.withTimeAtStartOfDay() ).getDays()
  
}


// define function to distill the sequentially typed queries into the end queries
import scala.annotation.tailrec
def shrinkList(originalList: List[String]): List[String] =  {
  
 @tailrec def loop(ol: List[String], acc:List[String]): List[String] = {

      if (ol.length<2) acc:::ol
  else if (ol.tail.head.startsWith(ol.head)) loop(ol.tail, acc)
  else if (ol.head.startsWith(ol.tail.head)) loop(ol.head +: ol.tail.tail, acc)
    else loop(ol.tail, acc :+ ol.head)
  }
    val el  = List[String]()
  loop(originalList, el)
};

// shrinkList(List("a", "at", "be", "bel", "belo", "belong", "belon", "bel", "bellicose", "genera","gener","gene","gen","physi", "physical"))


// <!-- user defined functions to transform dataframe columns -->
import org.apache.spark.sql.functions._

val toInt    = udf[Int, String]( _.toInt)
val toLong = udf[Long, String] ( _.toLong )
val toDouble = udf[Double, String]( _.toDouble)
val toHour   = udf((t: String) => "%04d".format(t.toInt).take(2).toInt ) 
val toLower = udf[String, String]( _.toLowerCase)
val days_since_nearest_holidays = udf( 
  (year:String, month:String, dayOfMonth:String) => year.toInt + 27 + month.toInt-12
 )

// val toAlias    = udf( _.alias(_.substring(_.lastIndexOf(".") + 1)))

import scala.collection.mutable.WrappedArray
import sqlContext.implicits._
val shrinkL = udf((arrayCol: WrappedArray[String]) => shrinkList(arrayCol.toList) )
val mkString = udf((arrayCol: WrappedArray[String]) => arrayCol.mkString("~"))  
val roundto2 = udf[Double, Double]( "%.3f".format(_).toDouble)
val firstIp = udf[String, String](ips => ips match { case null => null 
  case s => s.split(",")(0)}
)
// val extractDateAsOptionInt = udf((d: String) => d match {
//   case null => None
//   case s => Some(s.substring(0, 10).filterNot("-".toSet).toInt)
// })

val firstElement = udf( (arrayCol:Array[String]) => arrayCol(0))

def getPagination(pattern: scala.util.matching.Regex) = udf(
  (url: String) => pattern.findFirstMatchIn(url) match { 
    case Some(offset) => offset.group(1).toInt/10+1
    case None => 1
  }
)

val cleanQuery = udf[String, String](_.toLowerCase.trim.stripPrefix("\"").stripSuffix("\""))

// String filename = "abc.def.ghi";     // full file name
// int iend = filename.indexOf("."); //this finds the first occurrence of "." 
// //in string thus giving you the index of where it is in the string

// // Now iend can be -1, if lets say the string had no "." at all in it i.e. no "." is not found. 
// //So check and account for it.

// if (iend != -1) 
// String subString= filename.substring(0 , iend);

// object MatchTest1 extends App {
//   def matchTest(x: Int): String = x match {
//     case 1 => "one"
//     case 2 => "two"
//     case _ => "many"
//   }
//   println(matchTest(3))
// }



val AccessKey = "somekey"
val SecretKey = "somekey1"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val frontEnd = "bucketname"
val MountNameFE = "searchFront"
val backEnd = "bucketname2"
val MountNameBE = "searchBack"
val writeBucket = "bucketname3"
val writeMountName = "zocdoop2"


def mountBucket(dstBucketName:String, dstMountName:String) {
  import java.lang.IllegalArgumentException
  try {
    dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$dstBucketName", dstMountName) 
    println(s"Bucket $dstMountName is mounted")
  } catch {
    case e: java.rmi.RemoteException => {
      println("Already Mounted. Re-mount the bucket:")
      dbutils.fs.unmount(dstMountName)
      mountBucket(dstBucketName, dstMountName)
    }
    case e: Exception => {
      println("There was some other error")
    }
  }
}


mountBucket(frontEnd, s"/mnt/$MountNameFE")
mountBucket(backEnd, s"/mnt/$MountNameBE")
mountBucket(writeBucket, s"/mnt/$writeMountName")




class logsByDate (paths: List[String]) {
  
  val data = 
  if (paths.length < 2)
    sc.textFile(paths(0))
  else
   paths.map(path => sc.textFile(path)).reduce((a,b)=> a.union(b))
  
  val jsondata = sqlContext.read.json(data)
  
//   data.registerTempTable("data")
  
//   sc.textFile('file.csv')
//     .map(lambda line: (line.split(',')[0], line.split(',')[1]))
//     .collect()
}

// COMMAND ----------

// val fields = List("ag.get.gete", "tee.tw.ppe")
//   val fieldsWithAlias = fields.map(x => "$\"" + x +"\".alias(\""+ x.substring(x.lastIndexOf(".") + 1) +"\")").mkString(",")


// import org.apache.spark.sql.Column

// def aliasName(c: Column) = {
//   val colname = c.toString
//   c.alias(colname.substring(colname.lastIndexOf(".") + 1))
// }



// COMMAND ----------

import org.apache.spark.sql.types._

class logsByField (jsondata: org.apache.spark.sql.DataFrame, fields: List[String], filterTerm: String) {
  

//   val data = sqlContext.read.json(path)
  
//   jsondata.registerTempTable("data")
  
jsondata.createOrReplaceTempView("data")

val fieldsString = fields.mkString(",")
  

 val logs = {
   if (filterTerm == "") sqlContext.sql(s"SELECT $fieldsString FROM data") 
   else sqlContext.sql(s"SELECT $fieldsString FROM data").filter(filterTerm)
 }
  
  def findFields(path: String, dt: DataType): Unit = dt match {
  case s: ArrayType => 
  findFields(path, s.elementType)
  case s: StructType => 
    s.fields.foreach(f => findFields(path + "." + f.name, f.dataType))
  case other => 
    println(s"$path: $other")
}
  
  
  def getFieldsList(path: String, dt: DataType): Seq[String] = {

def loop(path: String, dt: DataType, acc:Seq[String]): Seq[String] = {
  dt match {
  case s: ArrayType =>
       loop(path, s.elementType, acc)
  case s: StructType =>      
    s.fields.flatMap(f => loop(path + "." + f.name, f.dataType, acc))
  case other => 
    acc:+ path
}
 
}
   loop(path, dt, Seq.empty[String])
}
  
  
    val allFields = getFieldsList("", jsondata.schema).map(_.stripPrefix("."))

  


  val nRecords = logs.count
  
  

}



import org.apache.spark.sql.types._

case class logsByFieldFront (jsondata: org.apache.spark.sql.DataFrame, fields: List[String], filterTerm: String) extends logsByField (jsondata, fields, filterTerm) {
  


  
val ipLogs = logs.withColumn("x-forwarded-for",firstIp($"x-forwarded-for")).filter("`x-forwarded-for` not in ('1.1.1.1', '2.2.2.2')").filter("`x-forwarded-for` not like '10.%'")


}





import org.apache.spark.sql.types._

case class logsByFieldBack (jsondata: org.apache.spark.sql.DataFrame, fields: List[String], filterTerm: String, expFields: List[String]) extends logsByField (jsondata, fields, filterTerm) {
  
val expLogs = expFields.foldLeft(logs)((acc, c) => acc.withColumn(c, explode(col(c))))
  

}


import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

class queryLogs ( val inputLogs: org.apache.spark.sql.DataFrame, queryFilterTerm: String, idField: String, queryField: String, sortField: String) {
  
  val filteredLogs = inputLogs.filter(queryFilterTerm).withColumn("cleanedQuery", cleanQuery(col(queryField)))
  filteredLogs.createOrReplaceTempView("logs")

  
  val topqs = sqlContext.sql(s"with x as (select cleanedQuery, count(*) as n from logs group by cleanedQuery order by n desc) select cleanedQuery, n from x limit 100")

  val sortedQueries = sqlContext.sql(s"select $idField, collect_list($queryField) queryList from (select * from logs sort by $sortField) x group by $idField")
  
println(sortedQueries.count)
  
  val nIds = sortedQueries.count
  
  val minQueries = sortedQueries.select(idField, "queryList").withColumn("queryList",shrinkL($"queryList"))

  
  val minQueryStrings = minQueries.select("queryList").withColumn("queryList",mkString($"queryList"))
  

  

val w = Window.partitionBy(idField).orderBy(desc(sortField))

val finalQueries = filteredLogs.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
  
val queryCounts = filteredLogs.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn").select("cleanedQuery").groupBy("cleanedQuery").count.sort(desc("count"))

  
}




class selectionLogs ( val inputLogs: org.apache.spark.sql.DataFrame, eventFilterTerm: String, idField: String, queryField: String, sortField: String) {
  
  val filteredLogs = inputLogs.filter(eventFilterTerm)
  filteredLogs.createOrReplaceTempView("logs")
  

  
}



class queryStats ( df: org.apache.spark.sql.DataFrame, idField: String, queryField: String, sortField: String) {
  



val queryCountStats = df.select(mean("count"), min("count"), max("count")).show()



  
}




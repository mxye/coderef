val AccessKey = "some key"
val SecretKey = "some secret key"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "lknbucket2"
val MountName = "lkn"


dbutils.fs.unmount(s"/mnt/$MountName")
dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")


// string replace
// wildcard in the path name
val yyyy: String = "2016"
val mm: String = "07"
val dd: String = "22"
val hh: String = "16"
val dayPath: String = s"/mnt/$MountName/newLkndata/$yyyy-$mm-$dd*/"

//display doesn't work but reading works
display(dbutils.fs.ls(dayPath))

//this reads all files under the dayPath
// val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//In Databricks, developers should utilize the shared HiveContext instead of creating one using the constructor. In Scala and Python notebooks, the shared context can be accessed as sqlContext. When running a job, you can access the shared context by calling SQLContext.getOrCreate(SparkContext.getOrCreate()).
val myApacheLogs = sqlContext.read.json(dayPath)
// in fact, if the wildcarded path matches multiple paths
// all files in all paths are read
val all2016Path: String = s"/mnt/searchtemp/$yyyy/*/*/*/*"
val myApacheLogs = sqlContext.read.json(all2016Path)

// count the number of records
myApacheLogs.count()

// show the schema
myApacheLogs.printSchema()

// register temp table to be used in sparkSQL
myApacheLogs.registerTempTable("people")
val ppl = sqlContext.sql("SELECT recruiter_name, count(*) FROM people group by recruiter_name")

// convert current time into numeric format
val currentTimestamp: Long = System.currentTimeMillis / 1000


// parse datetime 
import java.text.SimpleDateFormat
val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
val refDate = format.parse("2016-07-20T01:39:34.174Z")

// here the temp table people is myApacheLogs
// we found out that the timestamp field is a struct containing a String field called "s"
// show an array of the parsed dates
val time_col = sqlContext.sql("select timestamp.s from people")
                         .map(line => format.parse(line(0).toString))
time_col.take(3)

// RDD/dataFrame: filter records by the timestamp -- it was in string format, utc dates
val recentRecords = myApacheLogs.filter($"timestamp.s".gt("2016-07-22")).show()


// rdd to dataFrame:
rdd.toDF()

//dataFrame to rdd:
df.rdd



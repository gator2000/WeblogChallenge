/*
 * This script contains a sequence of scala statements that can be executed
 * on the scala REPL to "solve" the Weblog challenge.
 * The script assumes that the sample web log data has been loaded on HDFS
 * already, under the home directory of the current user.
 * Prior to loading to HDFS, the original file was decompressed, to make it
 * possible for Spark to process blocks in parallel, as gzip is not a
 * splittable compression format.
 */

// Create a plain text hadoop RDD with the raw data
val sample = sc.textFile("sample.log")

// Regex to parse log events, as per specs at:
// http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-form
// The regex recognizes either strings delimited by double quotes or sequences of characters that do not contain spaces
val regex = """("[^"]*")|([^\s]+)""".r

// Parse the data. This maps each line to an array of strings
val parsed = sample.map(li => regex.findAllIn(li).toIndexedSeq)

// Some basic exploratory queries showed that a few lines (22 to be exact) were not parsed correctly
// As per the format specs, 15 fields are expected for a well formed event. The 22 "bad" events have 17 fields
// The second condition filters out requests that were not served (their server time is -1)
val clean = parsed.filter(li => li.size == 15 && li(5).toDouble > 0)
clean.count

// Case class for the fields we will need to find answers
// minute = number of minutes since Unix epoch for the given timestamp
// clientId = client IP address (used as ID as per instructions)
// url = requested URL
// NOTE: the code below is *not* distinguishing among differnt kinds of HTTP requests (GET, POST, etc)
case class LogEvent(minute:Long, clientId:String, url:String)

// Create a DataFrame where the row type is derived from our case class
// We use the joda library to parse the dates in ISO format
// We get rid of the port number using  e(2).split(":")(0)
// We extract the URL of the request using e(11).split(" ")(1)
val eventsDf = clean.mapPartitions(p => {val parser = org.joda.time.format.ISODateTimeFormat.dateTimeParser; p.map(e => LogEvent(parser.parseDateTime(e(0)).getMillis / 60000, e(2).split(":")(0), e(11).split(" ")(1)))}).toDF

// Register the data frame as a temp table, so that we can query it using SQL
eventsDf.registerTempTable("events")

// Use the LAG window function within the events for a given client to find session boundaries
// A session boundary is an event that happens at least 15 minutes after another event
// Event representing a boudary are labeled 1 (otherwise 0)
val labeledEvents = sqlContext.sql("select clientId, minute, url, case when minute - lag(minute) over (partition by clientId order by minute) >= 15 then 1 else 0 end as label  from events")
:String, url:String)

labeledEvents.registerTempTable("labeledEvents")
sqlContext.sql("select count(*) from labeledEvents where label = 1").show

// Use cumulative SUM as window function to label each event with a "session id". The id is unique within each client group
// This completes GOAL 1
val sessions = sqlContext.sql("select clientId, minute, url, sum(label) over (partition by clientId order by minute) as sessionId from labeledEvents")
sessions.registerTempTable("sessions")
sqlContext.sql("select count(distinct sessionId) from sessions").show

// Compute the duration of each session
val sessionTimes = sqlContext.sql("select clientId, sessionId, (max(minute) - min(minute)) as duration from sessions group by clientId, sessionId")
sessionTimes.registerTempTable("sessionTimes")

// Find average session time
// This completes GOAL 2
sqlContext.sql("select avg(duration) from sessionTimes").show

// Determine unique URL visits per session
// This completes GOAL 3
val unique = sqlContext.sql("select clientId, sessionId, count (distinct url) from  sessions group by clientId, sessionId")
unique.take(10)
// res16: Array[org.apache.spark.sql.Row] = Array([101.2.37.28,0,58], [103.244.241.203,0,69], [59.180.168.165,0,13], [122.177.71.143,0,7], [124.125.53.133,0,5], [49.14.19.51,0,62], [182.73.162.66,0,25], [14.98.5.56,0,11], [70.39.187.219,3,71], [182.58.227.231,1,10])

// Get IPs with the longest session times
// This completes GOAL 4
val mostEngaged = sqlContext.sql("select clientId, max(duration) as maxDuration from sessionTimes group by  clientId order by maxDuration desc")
mostEngaged.take(10)
// res17: Array[org.apache.spark.sql.Row] = Array([121.58.175.128,34], [122.15.11.155,34], [125.20.39.66,34], [203.99.198.151,34], [121.243.8.100,34], [125.19.44.195,34], [203.99.198.19,34], [203.99.198.17,34], [183.82.103.131,34], [202.46.22.10,34])

import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
val logger = Logger.getRootLogger()

val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

logger.info("Creation des dataframes")
val trip_df = sqlContext.read.format("com.databricks.spark.csv").option("inferScheme","true").option("header","true").load("tripA_10-06-2021.csv");
val location_df = sqlContext.read.format("com.databricks.spark.csv").option("inferScheme","true").option("header","true").load("zoneA_11-06-2021.csv");
val vendor_df = sqlContext.read.format("com.databricks.spark.csv").option("inferScheme","true").option("header","true").load("vendorA_11-06-2021.csv");
val ratecode_df = sqlContext.read.format("com.databricks.spark.csv").option("inferScheme","true").option("header","true").load("ratecodeA_02-07-2021.csv");
val payment_df = sqlContext.read.format("com.databricks.spark.csv").option("inferScheme","true").option("header","true").load("paymentA_02-07-2021.csv");
logger.info("Fin de la creation des dataframes")

//trip_df.show()
//trip_df.printSchema()
//trip_df.select("VendorID").show()


// RAW DATA TABLES
logger.info("Creation des vues")

trip_df.createOrReplaceTempView("trip")
location_df.createOrReplaceTempView("location")
vendor_df.createOrReplaceTempView("vendor")
ratecode_df.createOrReplaceTempView("ratecode")
payment_df.createOrReplaceTempView("payment")

logger.info("fin de la creation des vues")

// SELECT
logger.info("Requêtes utiles")

val tDF = spark.sql("SELECT * FROM trip")
val lDF = spark.sql("SELECT * FROM location")
val vDF = spark.sql("SELECT * FROM vendor")
val rDF = spark.sql("SELECT * FROM ratecode")
val pDF = spark.sql("SELECT * FROM payment")

val trip_by_location_PULocationID = spark.sql("SELECT trip.VendorID,location.Borough,count(trip.PULocationID) as nb_trip FROM trip join location on (trip.PULocationID = location.LocationID) group by trip.VendorID,location.Borough")
val trip_by_location_DOLocationID = spark.sql("SELECT trip.VendorID,location.Borough,count(trip.DOLocationID) as nb_trip FROM trip join location on (trip.DOLocationID = location.LocationID) group by trip.VendorID,location.Borough")

logger.info("Nombres de trajets par localité de depart")
trip_by_location_PULocationID.show()
logger.info("Nombres de trajets par localité de d'arrivee")
trip_by_location_DOLocationID.show()

trip_by_location_DOLocationID.agg(max("nb_trip")).show()


val df = spark.sql("SELECT VendorID,PULocationID,DOLocationID,RatecodeID,passenger_count,payment_type FROM trip")
df.show()

//:load test.scala
//tDF.show()
//lDF.show()
//vDF.show()
//rDF.show()
//pDF.show()


//import org.apache.spark.graphx.Graph
///val g = Graph(tDF,defaultValue = 1)

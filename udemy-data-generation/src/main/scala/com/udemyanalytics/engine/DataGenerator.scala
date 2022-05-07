package com.udemyanalytics.engine

import com.udemyanalytics.processing.UdemyAnalyticsSparkSession
import com.udemyanalytics.utils.{ReadUtil, WriteUtil}
import org.apache.spark.sql.functions.{col, lit, udf, when}

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, Random}

/**
 * Main class to generate the data
 */
object DataGenerator extends App {

  //global random object used to generate random number
  val randomizer = new Random()

  //loading properties from the property file
  val props = new Properties()
  props.load(classOf[App].getClassLoader.getResourceAsStream("application.properties"))
  val MASTER_CONFIG = props.getProperty("master_config")
  val totalRecords = Integer.parseInt(props.getProperty("total_records"))
  val outputBasePath = props.getProperty("output_base_path")

  //creating spark session object
  val spark = UdemyAnalyticsSparkSession.getOrCreateSparkSessionObject(MASTER_CONFIG)

  //creating udf functions
  val binomialUdf = udf(binomialFunction(_:Double): Int)
  val epochGeneratorFromDateUdf = udf(epochGeneratorForDate(_:String, _:Int): Long)
  val epochGeneratorFromEpochUdf = udf(epochGeneratorForEpoch(_:String, _:Int): Long)
  val getEndDateUdf = udf(getEndDate(_:String): String)
  val getMarketingProductUdf = udf(getMarketingProduct(_:String): String)

  //read users data from the csv
  val usersDf = ReadUtil.readCsvFile(spark, this.getClass.getResource("/user_details/").toString, true)

  //read campaigns data from the csv
  val campaignsDf = ReadUtil.readCsvFile(spark,this.getClass.getResource("/campaign_details/").toString, true)
    .withColumn("course_campaign_end_date", getEndDateUdf(col("course_campaign_start_date")))
    .withColumn("marketing_product", getMarketingProductUdf(col("course_campaign_name")))


  //creating a list of user_id and campaign_id
  val userIdList = usersDf.select("user_id").distinct().collect().toList
  val campaignIdList = campaignsDf.select("campaign_id").distinct().collect().toList

  //creating a dataframe which will have random user_id mapped against campaign_id
  val campaignIdVsUserIdDF = spark.createDataFrame(generateCampaignUserData(totalRecords)).distinct()
    .withColumnRenamed("_1", "user_id")
    .withColumnRenamed("_2", "campaign_id")

  //joining the campaignIdVsUserIdDF with usersDf and campaignsDf to fetch all the column data
  val hubspotEmailSentEventInitial = campaignIdVsUserIdDF.join(usersDf,
    campaignIdVsUserIdDF.col("user_id") === usersDf.col("user_id"), "inner")
    .join(campaignsDf, campaignIdVsUserIdDF.col("campaign_id") === campaignsDf.col("campaign_id"), "inner")
    .drop(campaignsDf.col("campaign_id")).drop(usersDf.col("user_id"))

  //selecting relevant columns
  val hubspotEmailSentEventFiltered = hubspotEmailSentEventInitial
    .select("campaign_id", "course_campaign_name", "user_id",
      "user_name", "course_campaign_start_date", "user_last_activity", "digital_marketing_team")


  //generating data for hubspot_email_sent_event
  val hubspotEmailSentEventDf = hubspotEmailSentEventFiltered
    .withColumnRenamed("course_campaign_start_date", "campaign_date")
    .withColumn("event_status_temp", lit(.8D))
    .withColumn("event_status", binomialUdf(col("event_status_temp")))
    .withColumn("event_type", when(col("event_status") === "1", "email_sent")
      .otherwise("email_bounce"))
    .withColumn("marketing_product", lit("dummy_product"))
    .withColumn("user_response_time", when(col("event_status") === "1",
      epochGeneratorFromDateUdf(col("campaign_date"), lit(1)))
      .otherwise("-1"))
    .drop("event_status_temp")
    .drop("user_last_activity")

  //generating data for hubspot_email_open_event
  val hubspotEmailOpenEventDf = hubspotEmailSentEventDf.filter("event_status=1")
    .withColumn("event_status_temp", lit(.6D))
    .withColumn("event_status", binomialUdf(col("event_status_temp")))
    .withColumn("event_type", when(col("event_status") === "1", "email_open")
      .otherwise("email_no_open"))
    .withColumn("user_response_time", when(col("event_status") === "1",
      epochGeneratorFromEpochUdf(col("user_response_time"), lit(1))).otherwise("-1"))
    .drop("event_status_temp")

  //generating data for hubspot_email_click_event
  val hubspotEmailClickEventDf = hubspotEmailOpenEventDf.filter("event_status=1")
    .withColumn("event_status_temp", lit(.45D))
    .withColumn("event_status", binomialUdf(col("event_status_temp")))
    .withColumn("event_type", when(col("event_status") === "1", "email_click")
      .otherwise("email_no_click"))
    .withColumn("user_response_time", when(col("event_status") === "1",
      epochGeneratorFromEpochUdf(col("user_response_time"), lit(1))).otherwise("-1"))
    .drop("event_status_temp")

  //generating data for hubspot_email_unsubscribe_event
  val hubspotEmailUnsubscribeEventDf1 = hubspotEmailClickEventDf.filter("event_status=1")
    .withColumn("event_status_temp", lit(.1D))
    .withColumn("event_status", binomialUdf(col("event_status_temp")))
    .withColumn("event_type", when(col("event_status") === "1", "email_unsubscribe")
      .otherwise("email_no_unsubscribe"))
    .drop("event_status_temp")

  val hubspotEmailUnsubscribeEventDf2 = hubspotEmailOpenEventDf.filter("event_status=1")
    .withColumn("event_status_temp", lit(.30D))
    .withColumn("event_status", binomialUdf(col("event_status_temp")))
    .withColumn("event_type", when(col("event_status") === "1", "email_unsubscribe")
      .otherwise("email_no_unsubscribe"))
    .drop("event_status_temp")

  val hubspotEmailUnsubscribeEventDf = hubspotEmailUnsubscribeEventDf1
    .union(hubspotEmailUnsubscribeEventDf2)
    .withColumn("user_response_time", when(col("event_status") === "1",
      epochGeneratorFromEpochUdf(col("user_response_time"), lit(1))).otherwise("-1"))

  //writing the output to csv
  val emailSentOutputPath = outputBasePath + "hubspot_email_sent_event"
  WriteUtil.writeToCsv(hubspotEmailSentEventDf, emailSentOutputPath, true)

  val emailOpenOutputPath = outputBasePath + "hubspot_email_open_event"
  WriteUtil.writeToCsv(hubspotEmailOpenEventDf, emailOpenOutputPath, true)

  val emailClickOutputPath = outputBasePath + "hubspot_email_click_link_event"
  WriteUtil.writeToCsv(hubspotEmailClickEventDf, emailClickOutputPath, true)

  val emailClickUnsubscribePath = outputBasePath + "hubspot_email_unsubscribe_event"
  WriteUtil.writeToCsv(hubspotEmailUnsubscribeEventDf, emailClickUnsubscribePath, true)

  val campaignsPath = outputBasePath + "campaign_details"
  WriteUtil.writeToCsv(campaignsDf, campaignsPath, true)

  val usersPath = outputBasePath + "user_details"
  WriteUtil.writeToCsv(usersDf, usersPath, true)

  /**
   *
   * @param p  Probability of success. Must be between 0 to 1
   * @return  A random integer (0 or 1) depending of the probability of success
   */
  def binomialFunction (p:Double): Int = {
    var x = 0
    if (Math.random < p) x += 1
    x
  }

  /**
   * A random data generator which generates a list of tuple for (UserId, CampaignId)
   */
  def generateCampaignUserData(count: Int): List[(String, String)] = {
    val userDataLength = userIdList.length
    val campaignDataLength = campaignIdList.length
    Range(0, count).
      collect(x => (userIdList(randomizer.nextInt(userDataLength)),
        campaignIdList(randomizer.nextInt(campaignDataLength)))).toList
      .map(x => (x._1.get(0).toString, x._2.get(0).toString))
  }

  /**
   * Generates a random epoch time between the given input date and maximum
   * number of days after the given date
   */
  def epochGeneratorForDate(date: String, maxDayDifference: Int) : Long = {
    val dateFormatter = new SimpleDateFormat("MM/dd/yyyy")
    dateFormatter.parse(date).getTime / 1000 + randomizer.nextInt(86400 * maxDayDifference)
  }

  /**
   * Generates a random epoch time between the given input date and maximum
   * number of days after the given date
   */
  def epochGeneratorForEpoch(date: String, maxDayDifference: Int) : Long = {
    if (date == "-1") return -1
    java.lang.Long.parseLong(date) + randomizer.nextInt(86400 * maxDayDifference)
  }

  /**
   *
   * Takes campaign start date as input and returns the campaign end date
   * which is 15 days after the start date
   */
  def getEndDate(date: String) : String = {
    val dateFormatter = new SimpleDateFormat("MM/dd/yyyy")
    val parsedDate = dateFormatter.parse(date)
    val calendar = Calendar.getInstance()
    calendar.setTime(parsedDate)
    calendar.add(Calendar.DAY_OF_MONTH, 15)
    dateFormatter.format(calendar.getTime)
  }

  /**
   *
   * @param campaignName
   * @return marketing_product value by extracting the product field
   */
  def getMarketingProduct(campaignName: String) : String = {
    campaignName.substring(6, campaignName.lastIndexOf("_"))
  }

}

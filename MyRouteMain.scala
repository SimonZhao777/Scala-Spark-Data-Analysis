package HelloWorld

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import org.apache.avro.generic.GenericData.Record
import org.apache.camel.Expression
import org.apache.camel.main.Main
import org.apache.camel.scala.dsl.builder.RouteBuilderSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{DateDiff, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, Period, PeriodType}
import spire.std.long

import scala.xml.XML

/**
 * A Main to run Camel with MyRouteBuilder
 */
object MyRouteMain {

  def main(args: Array[String]) {
    /*val conf = new SparkConf().setAppName("myHelloWorld").setMaster("lacal")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);*/
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);


    /*val row1 = Row("1", "3, 4, 6, 8", "2016-12-22")
    val row2 = Row("2", "2, 0", "2016-12-22")
    val row3 = Row("3", "7", "2016-12-22")
    val theRdd = sc.makeRDD(Array(row1, row2, row3))


    val schema = StructType(
      StructField("key", IntegerType, false) ::
        StructField("value", DoubleType, false) ::
        StructField("d", DoubleType, false) :: Nil)
    val rawSampleTableDF = sqlContext.createDataFrame(theRdd, schema)
    val ss = Array("s1")
    val s1 = Seq("s2")
    rawSampleTableDF.dropDuplicates(s1).count()
    val s2 = rawSampleTableDF("ss")


    import sqlContext.implicits._ // for `toDF` and $""
    import org.apache.spark.sql.functions._ // for `when

    val a = 7.0.toDouble
    rawSampleTableDF.withColumn("s", $"a"/2).show()
    rawSampleTableDF.select().filter(datediff(rawSampleTableDF("s1"), rawSampleTableDF("s2")))
    rawSampleTableDF.orderBy("s").select("ssx", "ss")
    val raw2 = rawSampleTableDF.groupBy("xx").count().filter("'count' >= 2")
    rawSampleTableDF.join(raw2, rawSampleTableDF("ss").!==() .notEqual(raw2("ss")))

    rawSampleTableDF.groupBy("aed").agg(max(""), sum(""))
    rawSampleTableDF.select("ad").distinct().collect()
    rawSampleTableDF.withColumn("aa",($"power".lt(-40)).cast(IntegerType))

    case class Record(Hour:Integer, Category:String, TotalValue:Double)
    rawSampleTableDF.as[Record].groupBy($"hour").reduce((x,y) => if (x.TotalValue > y.TotalValue) x else y).show()
    rawSampleTableDF.distinct()
    rawSampleTableDF.groupBy("aa").agg($"aa",count("a"))
    rawSampleTableDF.selectExpr()*/

//    import sqlContext.implicits._ // for `toDF` and $""
//    import org.apache.spark.sql.functions._ // for `when
//
//
//    import org.apache.spark.mllib.linalg.{Vectors, Vector}
//    import org.apache.spark.sql.functions.monotonicallyIncreasingId
//    import org.apache.spark.sql.Row
//    import org.apache.spark.rdd.RDD
//
//    val rdd = sc.parallelize(List((1.0, 1, Vectors.dense(1.0, 2.0)), (0.0, 2, Vectors.dense(5.0, 6.0)), (1.0, 1, Vectors.dense(3.0, 4.0)), (3.0, 2, Vectors.dense(7.0, 8.0))))
//    val data = rdd.toDF("label", "id", "features")
//    println("data:")
//    data.show
//
//
//    val pairs: RDD[(String, (Long, String))] = uidOrderDateDF
//      .withColumn("index", monotonicallyIncreasingId)
//      .map{case Row(uid: String, orderdate: String, index: Long) =>
//        (uid, (index, orderdate))}
//
//    val rows = pairs.groupByKey.mapValues(xs => {
//
//      val vs = xs
//        .toArray
//        .sortBy(_._1)
//      var concatDate = ""
//
//
//
//      vs.foreach(concatDate += _._2 + "；")
//      concatDate
//
//        //.foreach(_._2) // Sort by row id to keep order
//        //.flatMap(_._2) // flatmap vector values
//
//      //Vectors.dense(vs) // return concatenated vectors
//
//    })
//     //val rows1 =  rows.map{case (uid, orderdates) => (label, id, v)} // Reshape
//
//    val grouped = rows.toDF("uid", "orderdates").show
//    grouped.orderBy(desc("ss"))

    /*val pairs: RDD[((Double, Int), (Long, Vector))] = data
      // Add row identifiers so we can keep desired order
      .withColumn("uid", monotonicallyIncreasingId)
      // Create PairwiseRDD where (label, id) is a key
      // and (row-id, vector is a value)
      .map{case Row(label: Double, id: Int, v: Vector, uid: Long) =>
      ((label, id), (uid, v))}

    val rows = pairs.groupByKey.mapValues(xs => {

      val vs = xs
        .toArray
        .sortBy(_._1) // Sort by row id to keep order
        .flatMap(_._2.toDense.values) // flatmap vector values

      Vectors.dense(vs) // return concatenated vectors

    }).map{case ((label, id), v) => (label, id, v)} // Reshape

    val grouped = rows.toDF("label", "id", "features")*/

    /*println("group sorted:")
    grouped.show

    import org.apache.spark.sql.functions.{collect_list, udf, lit}
    import org.apache.spark.sql
    val testGroupSorted = data.groupBy($"id").agg(count("id"), concat_ws(",", collect_list($"lebel").asc).alias("labels"))
    testGroupSorted.show*/

    //val ss = raw2. - rawSampleTableDF

    /* when syntax
    val joinCondition = when($"a.m_cd".isNull, $"a.c_cd"===$"b.c_cd").otherwise($"a.m_cd"===$"b.m_cd")
    val dfA = rawSampleTableDF
    val dfB = rawSampleTableDF
    dfA.as('a).join(dfB.as('b), joinCondition).show


    theRdd.take(9).foreach(println)


    dfA.map(row=> {
      row.getAs[String](0)
    })*/


    /*val weather =
      <rss>
        <channel>
          <title>Yahoo! Weather - Boulder, CO</title>
          <item>
            <title>Conditions for Boulder, CO at 2:54 pm MST</title>
            <forecast day="Thu" date="10 Nov 2011" low="37" high="58" text="Partly Cloudy"
                      code="29" />
          </item>
        </channel>
      </rss>

    println((weather\\"rss"\\"channel"\\"title").text)*/

    /*val startTime = System.currentTimeMillis()
    /* Feature Configurations */
    val featureConfig = XML.loadFile("src/main/resources/conf/ResourceTables")
    featureConfig.foreach(rootNode => {
      val prod_singleValueFeatureTables = rootNode\\"FeatureTables"\\"PorductFeatureTables"\\"SingleValueFeatureTables"\\"SingleValueFeatureTable"
      val prod_multiValueFeatureTables = rootNode\\"FeatureTables"\\"PorductFeatureTables"\\"MultiValueFeatureTables"\\"MultiValueFeatureTable"
      val featureMatchingTables = rootNode\\"FeatureTables"\\"FeatureMachingTables"\\"MatchingTable"

      /* Feature Matching Tables */
      featureMatchingTables.foreach(targetNode => {
        val tableTagName = (targetNode\\"@tableTagName").text
        val tableName = targetNode.text
        println(tableTagName, tableName)
      })

      /* Product Feature Tables */
      /* Single Value */
      prod_singleValueFeatureTables.foreach(targetNode => {
        val featureColName = (targetNode\\"@featureColName").text
        val featureRename = (targetNode\\"@featureRename").text
        val featureTableName = targetNode.text
        println(featureColName, featureRename, featureTableName)
        /*println(featureRename)
        println(targetNode.text/*.attribute("featureRename").getOrElse("")*/)*/
      })
      /* Multi Value */
      prod_multiValueFeatureTables.foreach(targetNode => {
        val featureMatchingTableTagName = (targetNode\\"@featureMatchingTableTagName").text
        val featuresRenamePrefix = (targetNode\\"@featuresRenamePrefix").text
        val featureTableName = targetNode.text
        println(featureMatchingTableTagName, featuresRenamePrefix, featureTableName)
      })
    })

    /* Config Manager */
    println()
    val configManager = XML.loadFile("src/main/resources/conf/ConfigManager")
    configManager.foreach(rootNode => {
      val configManagerItems = rootNode\\"ConfigManager"\\"ConfigItem"

      /* Config Items */
      configManagerItems.foreach(targetNode => {
        val itemName = (targetNode\\"@itemName").text
        val itemValue = targetNode.text
        println(itemName, itemValue)
      })
    })

    /* XGB Config Map */
    println()
    val confMap = scala.collection.mutable.Map[String,String]()
    val xgbConfigMap = XML.loadFile("src/main/resources/conf/XGBConfigMap")
    xgbConfigMap.foreach(rootNode => {
      val xgbConfigMapItems = rootNode\\"XGBConfigMap"\\"ConfigItem"

      /* Config Items */
      xgbConfigMapItems.foreach(targetNode => {
        val itemName = (targetNode\\"@itemName").text
        val itemValue = targetNode.text
        confMap(itemName) = itemValue
        //println(itemName, itemValue)
      })
    })
    confMap.foreach(println)//.foreach(println())//.toString()

    val sampleTable = (featureConfig\\"FeatureTables"\\"SampleTable").text
    println(sampleTable)

    val endTime = System.currentTimeMillis()
    println("Running time is: " + (endTime - startTime) + " miliseconds")
  }*/

    //println("hello world")

    //val days = Days.days(7)
    //val period = new Period(0,0,0,7,0,0,0,0, PeriodType.days())
    /*val now = DateTime.now()
    val date = now.minusDays(7) //- period  //()..withYear(2017).withMonthOfYear(7).withDayOfMonth()
    println(now.toDate.toString)
    println(date.toDate.toString)*/

    //println(Calendar.getInstance().getTime()..toString)

    /*val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = sdf.format(cal.getTime)
    //println(currentDate.toString)

    cal.setTime(sdf.parse("2016-03-12"))
    //println(cal.getTime.toString)
    cal.add(Calendar.DATE, -1)//.add(Calendar.DATE, 1)
    val addOneDay:String = sdf.format(cal.getTime)
    println(addOneDay)

    import org.apache.spark.sql.functions._ // for `when`
    val dateminus1 = udf{(date:String) => {
      cal.setTime(sdf.parse(date))
      cal.add(Calendar.DATE, -1)
      val aa = sdf.format(cal.getTime)
      aa
    }}

    val convertToRealProductLevel = udf{(prodLevel:String) =>{
      if(prodLevel == "0" || prodLevel == "1") return "0"
      else if(prodLevel == "64") return "2"
      else if(prodLevel == "2") "3"
      if(prodLevel == "4") "4"
      if(prodLevel == "8") "5"
      if(prodLevel == "16") "6"
      if(prodLevel == "32") "7"
    }}
    val sssss:Int = 34
    convertToRealProductLevel
    println(dateminus1("2017-02-11"))*/

    /*// 日期处理（产品表的时间d记录的是d-1那一天的产品列）
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = sdf.format(cal.getTime)
    // cal.setTime(sdf.parse("2017-02-17"))
    // cal.add(Calendar.DATE, 1)
    // val addOneDay:String = sdf.format(cal.getTime)


    // 全自动计算日期
    // 订单统计天数
    val orderDays = 10

    // 浏览统计天数
    val viewDays = 7

    // flag=1订单统计阈值
    val oneDevidedByFive = 1.000/5.0000

    // 全量表的取表日期（订单表，刷单表均为全量表）
    cal.setTime(sdf.parse(currentDate))
    cal.add(Calendar.DATE, -1)
    val date_temp = sdf.format(cal.getTime)
    val date = "'" + date_temp + "'"

    // 订单数据统计日期
    cal.setTime(sdf.parse(date_temp))
    cal.add(Calendar.DATE, -orderDays)
    val orderDateStart_temp = sdf.format(cal.getTime)
    val orderDateStart = "'" + orderDateStart_temp + "'"
    cal.setTime(sdf.parse(date_temp))
    cal.add(Calendar.DATE, -1)
    val orderDateEnd_temp = sdf.format(cal.getTime)
    val orderDateEnd = "'" + orderDateEnd_temp + "'"

    // 详情页浏览数据的日期（需要满足订单统计日期区间向后推N天的区间）
    cal.setTime(sdf.parse(orderDateStart_temp))
    cal.add(Calendar.DATE, 1-viewDays)
    val prodViewDateStart_temp = sdf.format(cal.getTime)
    val prodViewDateStart = "'" + prodViewDateStart_temp + "'"
    val prodViewDateEnd_temp = orderDateEnd_temp
    val prodViewDateEnd = "'" + prodViewDateEnd_temp + "'"

    // 团队游产品表日期区间（产品表的时间d记录的是d-1那一天的产品列）（产品表的时间区间需要覆盖详情页浏览表的日期）需要是浏览数据表日期向后推一天
    cal.setTime(sdf.parse(prodViewDateStart_temp))
    cal.add(Calendar.DATE, 1)
    val prodBaseInfoDateStart_temp = sdf.format(cal.getTime)
    val prodBaseInfoDateStart = "'" + prodBaseInfoDateStart_temp + "'"
    cal.setTime(sdf.parse(prodViewDateEnd_temp))
    cal.add(Calendar.DATE, 1)
    val prodBaseInfoDateEnd_temp = sdf.format(cal.getTime)
    val prodBaseInfoDateEnd = "'" + prodBaseInfoDateEnd_temp + "'"


//    val newExp = new Expression("ssss")
//    newExp.
//    val datediff = new DateDiff("2017-03-07", "2017-03-05")

    //Days.daysBetween()




    println("orderDays: " + orderDays)
    println("viewDays: " + viewDays)
    println("date: " + date)
    println("orderDateStart: " + orderDateStart)
    println("orderDateEnd: " + orderDateEnd)
    println("prodViewDateStart: " + prodViewDateStart)
    println("prodViewDateEnd: " + prodViewDateEnd)
    println("prodBaseInfoDateStart: " + prodBaseInfoDateStart)
    println("prodBaseInfoDateEnd: " + prodBaseInfoDateEnd)*/

//    val v1 = DateTime.now//.parse("2017-02-13")
//    println(v1.toString)
    //println(Date.parse("2017-02-13"))

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date1 = format.parse("2013-07-05")
    val date2 = format.parse("2013-07-06")
    val diff = date2.getTime() - date1.getTime()
    val diffSeconds = diff/1000
    val diffMinutes = diff/(60 * 1000)
    val diffHours = diff/(60 * 60 * 1000)
    val diffDays = diff/(60 * 60 * 1000 * 24)
    //val ss = new Date()
    //val cnt = date1.compareTo(date2)
    println(date1)
    println(date2)
    println(diff)
    println(diffSeconds)
    println(diffMinutes)
    println(diffHours)
    println(diffDays)
    println("abc!".dropRight(2))

    DateDiff(date1, date2)


//    val dates = "2013-07-06"
//    val datesSplit = dates.split(",")
//    datesSplit.foreach(println(_))
////    val aa = datesSplit(1)
////    aa
//    println(datesSplit.length)
//
//    dates.
  }

//  public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
//    long diffInMillies = date2.getTime() - date1.getTime();
//    return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
//  }
}


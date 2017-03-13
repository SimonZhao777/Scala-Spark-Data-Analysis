package HelloWorld

/**
  * Created by zhaowl on 2017/3/13.
  */
object DataAnalysis1 {
  // 分析产品 productpattern 对用户偏好的影响


  // 日期处理（产品表的时间d记录的是d-1那一天的产品列）
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



  /* 统计团队游下单用户 */

  // 团队游过滤条件
  val filter1 = "producttype='L' and productcategory in ('国内旅游', '出境旅游', '境内N日游', '境外N日游') and productpattern in ('跟团游', '私家团', '当地参团', '当地组团', '独立成团', '半自助游')"

  // 团队游产品表（产品表的时间区间需要覆盖详情页浏览表的日期）
  val productBaseInfoDF = sqlContext.sql("select productid,producttype,productcategory,productpattern,d from dw_sbu_vadmdb.product_baseinfo where d >= " + prodBaseInfoDateStart + " and d <= " + prodBaseInfoDateEnd + " and " + filter1) //////////////////////////////  1
  //productBaseInfoDF.show()
  //val productCount = productBaseInfoDF.count() // productCount: Long = 522305

  // 团队游产品表生成新的日期列p_d=日期d-1
  import sqlContext.implicits._ // for `toDF` and $""
  import org.apache.spark.sql.functions._ // for `when`
  // 自定义方程
  val dateminus1 = udf{(date:String) => {
    cal.setTime(sdf.parse(date))
    cal.add(Calendar.DATE, -1)
    sdf.format(cal.getTime)
  }}
  // 新增列
  val dateModifiedProductBaseInfoDF = productBaseInfoDF.withColumn("p_d", dateminus1($"d")).drop("d")
  //dateModifiedProductBaseInfoDF.show()



  // 订单表
  val filter2 = "orderstatus in ('已成交')"
  //val filter2 = "orderstatus in ('处理中','未提交','已成交(部分退订)','待处理','已成交(全部退订)','已成交','已取消','已收款')"
  val orderUserDF = sqlContext.sql("select orderid,pkg,serverfrom,uid,orderdate,orderstatus,d from dw_sbu_vadmdb.order_detailinfo where d=" + date + " and " + "orderdate >= " + orderDateStart + " and orderdate <= " + orderDateEnd + " and " + filter2)
  //val orderUserCount = orderUserDF.count() // orderUserCount: Long = 24561
  //orderUserDF.show() // 问题：有些pkg为‘0’


  // 团队游产品下单用户表
  import org.apache.spark.sql.types.StringType
  /**/
  val groupOrderUserDF = orderUserDF.join(dateModifiedProductBaseInfoDF, (orderUserDF("orderdate") === dateModifiedProductBaseInfoDF("p_d")) && (orderUserDF("pkg") === dateModifiedProductBaseInfoDF("productid").cast(StringType)), "inner")
  //groupOrderUserDF.show()
  //val groupOrderUserCount = groupOrderUserDF.count() // groupOrderUserCount: Long = 2267




  // 只下一张单的团队游用户表
  val userWithOnlyOneOrderDF = groupOrderUserDF.groupBy("uid").count().withColumnRenamed("uid", "o_uid").filter("count = 1")
  //userWithOnlyOneOrderDF.show()
  //val userWithOnlyOneOrderCount = userWithOnlyOneOrderDF.count()
  // 同一用户只下一张单的团队游订单表
  val orderOfOneOrderUserDF = groupOrderUserDF.join(userWithOnlyOneOrderDF, (groupOrderUserDF("uid") === userWithOnlyOneOrderDF("o_uid")), "inner").select("uid", "orderdate", "pkg", "productpattern") /////////////////////////////////////////////////////////////////////  1
  //orderOfOneOrderUserDF.show()
  //val orderOfOneOrderUserCount = orderOfOneOrderUserDF.count()



  // 用户在详情页浏览数据
  val userProdViewWithEmptyUidDF = sqlContext.sql("select uid,productid,d from dw_sbu_vadmdb.pkg_detail_pkg_base_daily where d>=" + prodViewDateStart + " and d<=" + prodViewDateEnd).withColumnRenamed("d", "v_d").withColumnRenamed("productid", "v_productid")
  val userProdViewWithEmptyUidCount = userProdViewWithEmptyUidDF.count()
  val userProdViewDF = userProdViewWithEmptyUidDF.filter("uid != ''")
  val userProdViewWithoutEmptyUidCount = userProdViewDF.count()
  //val userProdViewDF = sqlContext.sql("select uid,productid,d from dw_sbu_vadmdb.pkg_detail_pkg_base_daily where d>=" + prodViewDateStart + " and d<=" + prodViewDateEnd).withColumnRenamed("d", "v_d").withColumnRenamed("productid", "v_productid")
  //userProdViewDF.show()
  //val userProdViewCount = userProdViewDF.count() // userProdViewCount: Long = 379261

  // 团队游用户表详情页浏览表
  val groupTourUserViewDF = userProdViewDF.join(dateModifiedProductBaseInfoDF, (userProdViewDF("v_d") === dateModifiedProductBaseInfoDF("p_d")) && (userProdViewDF("v_productid") === dateModifiedProductBaseInfoDF("productid")), "inner").drop("v_d").drop("v_productid")
  //groupTourUserViewDF.show()
  //val groupTourUserViewCount = groupTourUserViewDF.count() // groupTourAvid1UserCount: Long = 15797

  //val groupTourUserViewGroupByUidAndProductLevel = groupTourUserViewDF.groupBy("uid", "productlevel").count().orderBy("uid").show()



  // 调制详情页浏览表，只留下需要的字段，改变某些column名称，及数据类型
  import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
  val modifiedGrTourUserViewDF1 = groupTourUserViewDF.select("uid", "productid", "productpattern", "p_d").withColumnRenamed("uid","v_uid").withColumnRenamed("productid","v_pkg").withColumnRenamed("productpattern","v_productpattern") //////////////////////////////////  3
  val modifiedGrTourUserViewDF2 = modifiedGrTourUserViewDF1.withColumn("v_pkg", modifiedGrTourUserViewDF1("v_pkg").cast(StringType)).filter("v_uid != ''")
  //modifiedGrTourUserViewDF2.show()

  // 订单表与详情页浏览表合并
  val orderAndProductViewDF = orderOfOneOrderUserDF.join(modifiedGrTourUserViewDF2, (orderOfOneOrderUserDF("uid") === modifiedGrTourUserViewDF2("v_uid")), "inner")
  //orderAndProductViewDF.show()


  // 下单7天之内的浏览数据(去掉下单的产品的浏览数据)
  val orderAndProductView7DaysDF = orderAndProductViewDF.filter("0 <= datediff(orderdate, p_d) and datediff(orderdate, p_d) < " + viewDays.toString).filter("pkg != v_pkg") /////////////////////////////////////////////////////////////////////////////// 不保留下单的浏览记录统计
  orderAndProductView7DaysDF.show()

  // 得到用户下单7天之内浏览的产品钻级分布
  val orderAndProdView7DayStat = orderAndProductView7DaysDF.groupBy("uid", "v_productpattern").count() //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////   1
  orderAndProdView7DayStat.show()



  // 得到每个用户浏览最多的行程天数表
  import org.apache.spark.sql.functions.{rowNumber, max, broadcast}
  val userAndProdLevelMaxViewDF = orderAndProdView7DayStat.groupBy($"uid".alias("m_uid")).agg(max($"count").alias("m_count"))
  userAndProdLevelMaxViewDF.show()
  val orderAndProdLevelMaxView7DaysDF = orderAndProdView7DayStat.join(broadcast(userAndProdLevelMaxViewDF),($"uid" === $"m_uid") && ($"count" === $"m_count"), "inner").drop("uid").drop("count")
  orderAndProdLevelMaxView7DaysDF.show()






  import sqlContext.implicits._ // for `toDF` and $""
  import org.apache.spark.sql.functions._ // for `when`

  /* mintraveldays行程天数解释： D-零天; G-一天; O-两天; P-三天; R-四天; S-五天; */

  // 生成订单与详情页浏览并表
  val newOrderAndProdViewDF = orderAndProdView7DayStat.withColumn("v_productpattern_1", when($"v_productpattern" === "跟团游", $"count").otherwise(0))   //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////   18
    .withColumn("v_productpattern_2", when($"v_productpattern" === "私家团", $"count").otherwise(0))
    .withColumn("v_productpattern_3", when($"v_productpattern" === "当地参团", $"count").otherwise(0))
    .withColumn("v_productpattern_4", when($"v_productpattern" === "当地组团", $"count").otherwise(0))
    .withColumn("v_productpattern_5", when($"v_productpattern" === "独立成团", $"count").otherwise(0))
    .withColumn("v_productpattern_6", when($"v_productpattern" === "半自助游", $"count").otherwise(0))
  //newOrderAndProdViewDF.show()
  newOrderAndProdViewDF.registerTempTable("temp")

  // 合并同uid的rows之后的订单与详情页浏览并表
  val singleUidOrderAndProdViewDF = sqlContext.sql("select uid,sum(v_productpattern_1),sum(v_productpattern_2),sum(v_productpattern_3),sum(v_productpattern_4),sum(v_productpattern_5),sum(v_productpattern_6) from temp group by uid") //////////////////////////////////////  18
    .withColumnRenamed("_c1", "v_productpattern_1")
    .withColumnRenamed("_c2", "v_productpattern_2")
    .withColumnRenamed("_c3", "v_productpattern_3")
    .withColumnRenamed("_c4", "v_productpattern_4")
    .withColumnRenamed("_c5", "v_productpattern_5")
    .withColumnRenamed("_c6", "v_productpattern_6")
    .withColumn("v_sum", $"v_productpattern_1" + $"v_productpattern_2" + $"v_productpattern_3" + $"v_productpattern_4" + $"v_productpattern_5" + $"v_productpattern_6")
  singleUidOrderAndProdViewDF.show()




  // 合并生成最终表
  val renamedSingleUidOrderAndProdViewDF = singleUidOrderAndProdViewDF.withColumnRenamed("uid", "v_uid")
  val completeDF = orderOfOneOrderUserDF.join(renamedSingleUidOrderAndProdViewDF, orderOfOneOrderUserDF("uid") === renamedSingleUidOrderAndProdViewDF("v_uid"), "inner").drop("v_uid").filter("v_sum > 0")
  completeDF.show()



  // add flag to complete table
  val flagedCompleteDF = completeDF.withColumn("flag", when((
    (($"productpattern" === "跟团游") and (($"v_productpattern_1".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))            //////////////////////////////////////////////////////////////////   18
      or (($"productpattern" === "私家团") and (($"v_productpattern_2".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))
      or (($"productpattern" === "当地参团") and (($"v_productpattern_3".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))
      or (($"productpattern" === "当地组团") and (($"v_productpattern_4".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))
      or (($"productpattern" === "独立成团") and (($"v_productpattern_5".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))
      or (($"productpattern" === "半自助游") and (($"v_productpattern_6".cast(DoubleType))/($"v_sum".cast(DoubleType)) >= oneDevidedByFive))
    ), 1).otherwise(0))
  flagedCompleteDF.show(100)


  // 下单钻级是浏览最多的钻级时max_flag=1
  val prodLevelMaxViewFlagCompleteDF = flagedCompleteDF.join(orderAndProdLevelMaxView7DaysDF, ($"uid" === $"m_uid") && ($"productpattern" === $"v_productpattern"), "inner")   ///////////////////////////////////////////////////////////////////////////////////////////    2
  prodLevelMaxViewFlagCompleteDF.show(100)


  val v1:Double = flagedCompleteDF.filter("flag =1").count()
  val v2:Double = flagedCompleteDF.count()
  val v3:Double = v1/v2

  val v4:Double = prodLevelMaxViewFlagCompleteDF.count()
  val v5:Double = v4/v2

  println("========================================")
  println("订单数据统计日期： " + orderDateStart + " ~ " + orderDateEnd)
  println("详情页浏览数据统计日期： " + prodViewDateStart + " ~ " + prodViewDateEnd)
  println("详情页浏览数据包含空uid数量: " + userProdViewWithEmptyUidCount)
  println("详情页浏览数据包去除空uid数量: " + userProdViewWithoutEmptyUidCount)
  println("详情页统计浏览天数： " + viewDays)
  println("flag=1统计阈值: " + oneDevidedByFive)
  println("订单productpattern大于浏览productpattern分布阈值的订单数量V1: " + v1)   /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////   1
  println("总订单统计量V2: " + v2)
  println("V1/V2百分比: " + v3)
  println("订单productpattern浏览量为最大的订单数量(浏览统计不保留对下单产品的浏览统计): " + v4)          /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////   1
  println("V4/V2百分比: " + v5)
  println("========================================")

}

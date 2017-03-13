package HelloWorld

/**
  * Created by zhaowl on 2017/3/13.
  */
object OrderedProdPropertyViewMaxPercentage {
  // 分析下单产品某属性为唯一浏览最多的产品属性的比例


  // 日期处理（产品表的时间d记录的是d-1那一天的产品列）
  import java.text.SimpleDateFormat
  import java.util.Calendar
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val cal = Calendar.getInstance()
  //val currentDate = sdf.format(cal.getTime)
  val currentDate = "2017-03-02"
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
  val productBaseInfoDF = sqlContext.sql("select productid,producttype,productcategory,productpattern,productlevel,d from dw_sbu_vadmdb.product_baseinfo where d >= " + prodBaseInfoDateStart + " and d <= " + prodBaseInfoDateEnd + " and " + filter1) /////////////////////////  1
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
  val orderOfOneOrderUserDF = groupOrderUserDF.join(userWithOnlyOneOrderDF, (groupOrderUserDF("uid") === userWithOnlyOneOrderDF("o_uid")), "inner").select("uid", "orderdate", "pkg", "productlevel") /////////////////////////////////////////////////////////////////////  1
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
  val modifiedGrTourUserViewDF1 = groupTourUserViewDF.select("uid", "productid", "productlevel", "p_d").withColumnRenamed("uid","v_uid").withColumnRenamed("productid","v_pkg").withColumnRenamed("productlevel","v_productlevel") //////////////////////////////////  3
  val modifiedGrTourUserViewDF2 = modifiedGrTourUserViewDF1.withColumn("v_pkg", modifiedGrTourUserViewDF1("v_pkg").cast(StringType)).filter("v_uid != ''")
  //modifiedGrTourUserViewDF2.show()

  // 订单表与详情页浏览表合并
  val orderAndProductViewDF = orderOfOneOrderUserDF.join(modifiedGrTourUserViewDF2, (orderOfOneOrderUserDF("uid") === modifiedGrTourUserViewDF2("v_uid")), "inner")
  //orderAndProductViewDF.show()


  // 下单7天之内的浏览数据(去掉下单的产品的浏览数据)
  val orderAndProductView7DaysDF = orderAndProductViewDF.filter("0 <= datediff(orderdate, p_d) and datediff(orderdate, p_d) < " + viewDays.toString)//.filter("pkg != v_pkg") /////////////////////////////////////////////////////////////////////////////// 不保留下单的浏览记录统计
  orderAndProductView7DaysDF.show()

  // 得到用户下单7天之内浏览的产品钻级分布
  val orderAndProdView7DayStat = orderAndProductView7DaysDF.groupBy("uid", "productlevel", "v_productlevel").count() //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////   2
  orderAndProdView7DayStat.show()



  // 得到每个用户浏览最多的行程天数表
  import org.apache.spark.sql.functions.{rowNumber, max, broadcast}
  val userAndProdLevelMaxViewDF = orderAndProdView7DayStat.groupBy($"uid".alias("m_uid")).agg(max($"count").alias("m_count"))
  //userAndProdLevelMaxViewDF.show()
  val orderAndProdLevelMaxView7DaysDF = orderAndProdView7DayStat.join(broadcast(userAndProdLevelMaxViewDF),($"uid" === $"m_uid") && ($"count" === $"m_count"), "inner").drop("uid").drop("count")
  orderAndProdLevelMaxView7DaysDF.show()





  // 得到浏览最多的特征大于等于2的用户 及特征 及特征浏览量
  // val maxviewUser = orderAndProdLevelMaxView7DaysDF.groupBy($"m_uid".alias("n_uid")).count()
  // val maxViewUserDF = orderAndProdLevelMaxView7DaysDF.join(maxviewUser, orderAndProdLevelMaxView7DaysDF("m_uid") === maxviewUser("n_uid"), "inner")
  // maxViewUserDF.show()

  val maxviewMoreThan1User =  orderAndProdLevelMaxView7DaysDF.groupBy($"m_uid".alias("n_uid")).count().filter("count > 1")
  val maxViewMoreThan1UserDF = orderAndProdLevelMaxView7DaysDF.join(maxviewMoreThan1User, orderAndProdLevelMaxView7DaysDF("m_uid") === maxviewMoreThan1User("n_uid"), "inner")
  maxViewMoreThan1UserDF.show()


  // 得到浏览最多的特征为下单的特征
  val orderUserMaxViewDF = orderAndProdView7DayStat.join(orderAndProdLevelMaxView7DaysDF, (orderAndProdView7DayStat("uid") === orderAndProdLevelMaxView7DaysDF("m_uid")) && (orderAndProdView7DayStat("productlevel") === orderAndProdLevelMaxView7DaysDF("v_productlevel")), "inner") ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    2
  orderUserMaxViewDF.show()


  // 得到浏览最多的特征为下单的特征，且浏览最多的特征大于等于2的用户列表
  val orderUserMaxViewMoreThan1DF = orderAndProdView7DayStat.join(maxViewMoreThan1UserDF, (orderAndProdView7DayStat("uid") === maxViewMoreThan1UserDF("m_uid")) && (orderAndProdView7DayStat("productlevel") === maxViewMoreThan1UserDF("v_productlevel")), "inner")   //////  2
  orderUserMaxViewMoreThan1DF.show()


  val v1:Double = orderUserMaxViewMoreThan1DF.count()
  val v2:Double = orderUserMaxViewDF.count()
  val ratio = v1/v2

  println("========================================")
  println("特征： productlevel")
  println("订单日期：" + orderDateStart_temp +"~" + orderDateEnd_temp)
  println("统计浏览天数: " + viewDays)
  println("浏览最多的特征数量大于1的用户数v1： " + v1)
  println("所有浏览最多的特征等于订单特征的用户数v2: " + v2)
  println("占比v1/v2：" + ratio)
  println("========================================")


}

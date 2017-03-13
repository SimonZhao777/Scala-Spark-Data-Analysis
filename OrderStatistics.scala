package HelloWorld

/**
  * Created by zhaowl on 2017/3/13.
  */
object OrderStatistics {
  // 分析最近90天的用户订单数量


  // 日期处理（产品表的时间d记录的是d-1那一天的产品列）
  import java.text.SimpleDateFormat
  import java.util.Calendar
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val cal = Calendar.getInstance()
  val currentDate = sdf.format(cal.getTime)
  /* Usage example
  cal.setTime(sdf.parse("2017-02-17"))
  cal.add(Calendar.DATE, 1)
  val addOneDay:String = sdf.format(cal.getTime)
  */


  // 全自动计算日期
  // 订单统计天数
  val orderDays = 90


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


  // 团队游产品表日期区间（产品表的时间d记录的是d-1那一天的产品列）（产品表的时间区间需要覆盖详情页浏览表的日期）需要是浏览数据表日期向后推一天
  cal.setTime(sdf.parse(orderDateStart_temp))
  cal.add(Calendar.DATE, 1)
  val prodBaseInfoDateStart_temp = sdf.format(cal.getTime)
  val prodBaseInfoDateStart = "'" + prodBaseInfoDateStart_temp + "'"
  cal.setTime(sdf.parse(orderDateEnd_temp))
  cal.add(Calendar.DATE, 1)
  val prodBaseInfoDateEnd_temp = sdf.format(cal.getTime)
  val prodBaseInfoDateEnd = "'" + prodBaseInfoDateEnd_temp + "'"



  /* 统计团队游下单用户 */

  // 团队游过滤条件
  val filter1 = "producttype='L' and productcategory in ('国内旅游', '出境旅游', '境内N日游', '境外N日游') and productpattern in ('跟团游', '私家团', '当地参团', '当地组团', '独立成团', '半自助游')"

  // 团队游产品表（产品表的时间区间需要覆盖详情页浏览表的日期）
  val productBaseInfoDF = sqlContext.sql("select productid,producttype,productcategory,productpattern,d from dw_sbu_vadmdb.product_baseinfo where d >= " + prodBaseInfoDateStart + " and d <= " + prodBaseInfoDateEnd + " and " + filter1) //////////////////////////////  1


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



  // 订单表
  val filter2 = "orderstatus in ('已成交')"
  //val filter2 = "orderstatus in ('处理中','未提交','已成交(部分退订)','待处理','已成交(全部退订)','已成交','已取消','已收款')"
  val orderDF = sqlContext.sql("select orderid,pkg,serverfrom,uid,orderdate,orderstatus,d from dw_sbu_vadmdb.order_detailinfo where d=" + date + " and " + "orderdate >= " + orderDateStart + " and orderdate <= " + orderDateEnd + " and " + filter2)
  //orderUserDF.show() // 问题：有些pkg为‘0’


  // 团队游产品下单表
  import org.apache.spark.sql.types.StringType
  val groupOrderDF = orderDF.join(dateModifiedProductBaseInfoDF, (orderDF("orderdate") === dateModifiedProductBaseInfoDF("p_d")) && (orderDF("pkg") === dateModifiedProductBaseInfoDF("productid").cast(StringType)), "inner")
  println("团队游订单表")
  //groupOrderDF.show()



  /* 统计团队游刷单用户 */
  // 团队游刷单用户表 (对uid去重)
  val filter3 =  "productcategoryname in ('国内旅游', '出境旅游', '境内N日游', '境外N日游') and productpatternname in ('跟团游', '私家团', '当地参团', '当地组团', '独立成团', '半自助游')"
  val groupTourScalpingUserDF = sqlContext.sql("select orderid,productid,uid,orderdate,productcategoryname,productpatternname,d from dmart_sbu_vadmdb.anticf_clickfarming_uid_full where d=" + date + " and " + "orderdate>=" + orderDateStart + " and orderdate<=" + orderDateEnd + " and " + filter3).dropDuplicates(Array("uid")).withColumnRenamed("uid", "s_uid").drop("uid")
  println("团队游刷单表")



  /* 统计团队游下单-刷单用户的订单 */
  // val uidRenamedGroupTourScalpingUserDF = groupTourScalpingUserDF.withColumnRenamed("uid", "s_uid")
  val realGroupTourOrderDF = groupOrderDF.join(groupTourScalpingUserDF, (groupOrderDF("orderdate") === groupTourScalpingUserDF("orderdate")) && (groupOrderDF("uid") === groupTourScalpingUserDF("s_uid")), "left").filter("s_uid is null").drop(groupTourScalpingUserDF("orderid")).drop(groupTourScalpingUserDF("productid")).drop(groupTourScalpingUserDF("s_uid")).drop(groupTourScalpingUserDF("orderdate")).drop(groupTourScalpingUserDF("productcategoryname")).drop(groupTourScalpingUserDF("productpatternname")).drop(groupTourScalpingUserDF("d"))
  println("团队游订单-刷单用户的订单")
  //realGroupTourOrderDF.show()




  /* 统计团队游下单-刷单用户的订单-Agent的订单*/
  //agent 用户ID列表
  val agentDF = sqlContext.sql("select * from dw_vacmldb.tour_user_agent where d=" + date).filter("value = '1'")
  // 团队游下单-刷单用户的订单-Agent的订单
  val realGroupTourOrderDF2 = realGroupTourOrderDF.join(agentDF, realGroupTourOrderDF("uid") === agentDF("key"), "left").filter("key is null").drop(agentDF("key")).drop(agentDF("value")).drop(agentDF("d"))
  println("团队游订单-刷单用户的订单-Agent的订单")
  //realGroupTourOrderDF2.show(100)



}

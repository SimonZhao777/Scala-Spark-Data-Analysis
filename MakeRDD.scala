package HelloWorld

/**
  * Created by zhaowl on 2017/3/13.
  */
object MakeRDD {
  import org.apache.spark.sql.{Row}
  import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.types._
  import org.apache.spark.{SparkConf, SparkContext}

  val row1 =Row("1", "1, 2, 3", "2016-12-22")
  val row2 = Row("2", "1, 3", "2016-12-22")
  val row3 = Row("3", "", "2016-12-22")
  //val row4 = Row("4", "3", "2016-12-22")
  val theRdd = sc.makeRDD(Array(row1, row2, row3/*, row4*/))
  /* ANOTHER WAY OF MAKING RDD
  val list_of_rows = Array(row1, row2, row3, row4, row5, row6, row7, row8, row9).toList
      val theRdd = sparkContext.parallelize(list_of_rows)*/
  /*case class X(id: Integer, indices: Double, weights: Double )
  val df=theRdd.map{
      case Row(s0,s1,s2)=>X(s0.asInstanceOf[Integer],s1.asInstanceOf[Double],s2.asInstanceOf[Double])
    }.toDF()
  df.show()*/
  val schema = StructType(StructField("key", StringType, false) ::
    StructField("value", StringType, false) ::
    StructField("d", StringType, false) :: Nil)
  val rawSampleTableDF = sqlContext.createDataFrame(theRdd, schema)
  rawSampleTableDF.show()

  import scala.collection.mutable.ArrayBuffer

  val ids = rawSampleTableDF.map(row => row.getString(0)).collect()
  val reArrangedRDD = rawSampleTableDF.map(row => {
    val key = row.getAs[String](0)
    val value = row.getAs[String](1).trim
    val d = row.getAs[String](2)
    val stringArray = value.split("\\s*,\\s*")

    stringArray.toString()


    val valuesBuf = ArrayBuffer.empty[Any]
    valuesBuf += key
    for(i <- ids) {
      valuesBuf += {if(stringArray.contains(i.toString))  1 else 0}
    }
    valuesBuf += d

    Row.fromSeq(valuesBuf.toSeq)
    //Row(key, if(stringArray.contains("1")) 1 else 0, if(stringArray.contains("2")) 1 else 0, if(stringArray.contains("3")) 1 else 0, d)
  })

  reArrangedRDD.take(4).foreach(println)

  /*val ss = " 1, 3, 2, 5  "
  val ssArray = ss.trim()
  val ssArraySplit = ssArray.split(",").map(_.trim)*/


  val schema2 = StructType(StructField("key", StringType, false) ::
    StructField("v1", IntegerType, false) ::
    StructField("v2", IntegerType, false) ::
    StructField("v3", IntegerType, false) ::
    StructField("d", StringType, false) :: Nil)
  val reArrangedDF = sqlContext.createDataFrame(reArrangedRDD, schema2)
  reArrangedDF.show()

  //reArrangedRDD.take(4).foreach(println)

}

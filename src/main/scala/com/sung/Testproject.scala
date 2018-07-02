package com.sung

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Round
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.zookeeper.Op.Create

object Testproject {

  /////////////////spark 연동 /////////////////////////
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("hbProject").
        config("spark.master", "local").
        getOrCreate()

//    Oracle 접속 ///

    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var seasonality = "KOPO_CHANNEL_SEASONALITY_NEW"
    var master = "kopo_product_master"


    var seasonalityDf = spark.read.format("jdbc").
      options(Map("url"->staticUrl,"dbtable"->seasonality,"user"->staticUser,"password"->staticPw)).load

    var masterDf = spark.read.format("jdbc").
      options(Map("url"->staticUrl,"dbtable"->master,"user"->staticUser,"password"->staticPw)).load



    //     데이터 프레임 확인
    seasonalityDf.show(1)
    masterDf.show(1)


//     tempView 생성

    seasonalityDf.createOrReplaceTempView("seasonalityTem")
    masterDf.createOrReplaceTempView("masterTem")

// join 하면서 담 테이블에 담기

    var joinTable = spark.sql("select B.*,A.PRODUCTNAME " +
                              "FROM masterTem A " +
                              "INNER JOIN seasonalityTem B " +
                              "ON A.PRODUCTID = B.PRODUCT")

    //   TempView 담기

//    var TestTable = spark.sql("select * from TestTableView")


//    TempView Table 확인

    TestTable.show(1)


//     인덱스 생성을 위한 컬럼 재정의

    var Testindexs = TestTable.columns

//    인덱스 생성

    var resionidNo = Testindexs.indexOf("REGIONID")
    var productNo = Testindexs.indexOf("PRODUCT")
    var yearweekNo = Testindexs.indexOf("YEARWEEK")
    var qtyNo = Testindexs.indexOf("QTY")


//    인덱스 생성 확인
    Testindexs(qtyNo)


// 데이터 분석을위해 Rdd 변환

    var TestRdd = TestTable.rdd

//    변환 한 Rdd 확인

//    첫줄확인
    TestRdd.first

//    전체 확인
    TestRdd.collect.foreach(println)

//    round 처리

    var yearRound = TestRdd.filter(x=>{
      var checkValid1 = true

      var Rounds = x.getDouble(qtyNo)

      var Round1 = math.round(Rounds)

      checkValid1

    })



//    정제 연산  yearweek 의 길이가 6자리 아닌경우 정제
    var yearweek6 = TestRdd.filter(x=>{

      var checkValid = true

      var yearweek = x.getString(yearweekNo)

      if(yearweek.length != 6){
        checkValid = false
      }
      checkValid
    })

//    결과

    yearweek6.count // 3611

//    정제 전 데이터

    TestRdd.count // 3611

//    결론 변화 없음 모두 길이가 6자리 였다


//    디버그 하기

    var yearweek6D = TestRdd

    var x = yearweek6D.first

    var yearweek = x.getString(yearweekNo)


//     정제 연산 연주차 정보 52 보다 큰값 제거

    var week52ft = TestRdd.filter(x=>{

      var Check = false // 기본 데이터들을 버리고 시작 if 문을 타고 들어가면 check true 를 만나 if 문을 타고 들어간 데이터 만 남는다.

      var yearweek = x.getString(yearweekNo)

      var week = yearweek.substring(4).toInt

      if(week < 52){
        Check = true
      }
      Check
    })


//    상품정보가 product1,2 인 정보만 필터링

    var productArray = Array("PRODUCT1","PRODUCT2").toSet// Set 타입으로 변환 집합타입

    var product1_2 = TestRdd.filter(x=>{

      var Check = false
      var qty = math.round(x.getDouble(qtyNo)).toDouble

      var product = x.getString(productNo)

      if(productArray.contains(product)){// 집합의 원소를 검사할수있는 contains 를 사용하여 if 문 통과 시키기
        Check = true
      }
      Check
    })


//    Rdd 가공 연산
    var RddRow = TestRdd.map(x=>{
      var qty = x.getDouble(qtyNo)
      var qty1 = math.round(qty)
      if(qty1 > 20000){ qty1 = 30000}
      Row(x.getString(resionidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty1)
    })

    RddRow.first

    var Rddbasic = TestRdd.map(x=>{
      var qty = x.getDouble(qtyNo)
      if(qty > 700000){ qty = 700000}
      Row(x.getString(resionidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty)
    })

    Rddbasic.first




//    RDD -> DataFrame 변경
// 사용할때 import 해야함
// import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

    var EndDf = spark.createDataFrame(week52ft,StructType
    (
        Seq(
          StructField("regionid",StringType),
          StructField("product",StringType),
          StructField("yearweek",StringType),
          StructField("qty",DoubleType)
        )
      ))


























  }



}

package com.sung

import org.apache.spark.sql.SparkSession
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
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW_A01"


    var TestDataFrame = spark.read.format("jdbc").
      options(Map("url"->staticUrl,"dbtable"->selloutDb,"user"->staticUser,"password"->staticPw)).load


//     데이터 프레임 확인
    TestDataFrame.show(1)


//     tempView 생성

    TestDataFrame.createOrReplaceTempView("TestTableView")

//   TempView 담기

    var TestTable = spark.sql("select * from TestTableView")


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


    var yearweek6DG = TestRdd.filter(x=>{

      var checkValid = true

      var yearweek = x.getString(yearweekNo)

      if(yearweek.length != 6){
        checkValid = false
      }
      checkValid
    })






















  }



}

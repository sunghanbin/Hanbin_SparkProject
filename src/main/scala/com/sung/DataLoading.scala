package com.sung

////////////// spark 연동 라이브러리 //////////
import org.apache.spark
import org.apache.spark.sql.SparkSession


object DataLoading {

  /////////////////spark 연동 /////////////////////////
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("hbProject").
        config("spark.master", "local").
        getOrCreate()

    ///////// local에서 로딩 하기 ///////////////

    var HOME = "./data/" // 위치를 변수로 설정
    var File = "kopo_channel_seasonality.csv"

   // 상대경로

    var TestData = spark.read.format("csv").option("header","true").load(HOME+File)
                  //데이터 프레임으로 변환

   // 절대 경로 //하드코딩??

    var TestData2 = spark.read.format("csv").option("header","true").load("c:/spark/bin/data/"+File)

    // 메모리 테이블 생성

    TestData2.createOrReplaceTempView("TestTable")

    // 메모리 테이블 볼때 는 프린트 문 사용
    println(TestData2)




    //option 에서 true 는 기존파일의 해더(컬럼명)를 사용하겠다는 뜻 / false 는 컬럼을 만들어 준다


    //  옵션 true
    //      +--------+------------+--------+-------+
    //      |REGIONID|PRODUCTGROUP|YEARWEEK|    VOL|
    //      +--------+------------+--------+-------+
    //      |     A01|      ST0002|  201512|151,750|
    //      |     A01|      ST0001|  201520|645,626|
    //      |     A01|      ST0002|  201520|125,863|
    //      |     A01|      ST0001|  201515|810,144|
    //      |     A01|      ST0002|  201515|128,999|
    //      +--------+------------+--------+-------+


    // 옵션 false

//      +--------+------------+--------+-------+
//      |     _c0|         _c1|     _c2|    _c3|
//      +--------+------------+--------+-------+
//      |REGIONID|PRODUCTGROUP|YEARWEEK|    VOL|
//      |     A01|      ST0002|  201512|151,750|
//      |     A01|      ST0001|  201520|645,626|
//      |     A01|      ST0002|  201520|125,863|
//      |     A01|      ST0001|  201515|810,144|
//      +--------+------------+--------+-------+


    ///////////////////////////////////////////////////////


  }







}

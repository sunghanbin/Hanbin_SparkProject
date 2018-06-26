package com.sung

////////////// spark 연동 라이브러리 //////////
import org.apache.spark
import org.apache.spark.sql.SparkSession


object DataLoading_Save {

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
                                                                     // ; 로 나눈다
    // 메모리 테이블 생성.option("Delimiter",";")

    TestData2.createOrReplaceTempView("TestTable")

    // 메모리 테이블 볼때 는 프린트 문 사용
    println(TestData2.show)




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



    // option("Delimiter",";") 를 사용했을 때
//      +----------------+--------------------+
//      |      PARAM_NAME|         PARAM_VALUE|
//      +----------------+--------------------+
//      |productgroupList|REF,TV,MOBILE,WM,APS|
//      |         myorder|                  17|
//      |          myIter|                   3|
//      | resultTableName|       SEASON_RESULT|
//      |       ap2idList|300114,300122,300130|
//      |           jobid|             batch_s|
//      |          prefix|                 brt|
//      +----------------+--------------------+





    //    기존 데이터를 Delimiter 옵션을 사용하지 않고 가져왔을때
//      +----------------------+
//      |PARAM_NAME;PARAM_VALUE|
//      +----------------------+
//      |  productgroupList;REF|
//      |            myorder;17|
//      |              myIter;3|
//      |  resultTableName;S...|
//      |      ap2idList;300114|
//      |         jobid;batch_s|
//      |            prefix;brt|
//      +----------------------+
    ///////////////////////////////////////////////////////



///////////////// DB에 접속하여 데이터 가져오기 ////////////////////////

//    Postgres/greenplumDB //

    //접속정보 설정
    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutData = "kopo_channel_seasonality"

    //jdbc(java database connectivity)연결 자바에서 데이터 베이스를 연결해주는 라이브러리
    var selloutDataFromPg = spark.read.format("jdbc").
      options(Map("url"->staticUrl,"dbtable"->selloutData,"user"->staticUser,"password"->staticPw)).load

    //메모리 테이블 생성
    selloutDataFromPg.createOrReplaceTempView("selloutTable")

    selloutDataFromPg.show(1)

    ////////////////////////////

    /// Mysql //
                        //:접속db이름:ip주소//포트번호/테이블스페이스명??
    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var staticUser = "root"// 계정
    var staticPw = "P@ssw0rd"// 비번
    var selloutDb = "KOPO_PRODUCT_VOLUME"// db 에서 가져올 데이터 이름

    //이하 동일


    // Oracle //

    var staticUrl = "jdbc:oracle:thin:@192.168.110.16:1522/XE"
    var staticUser = "haiteam"
    var staticPw = "haiteam"
    var selloutDb = "kopo_product_volume"

    // 이하 동일


    // Sqlserver(Microsoft SQL) //
    var staticUrl = "jdbc:sqlserver://192.168.110.70:1433;databaseName=kopo"//하나 다름 databaseName=kopo
    var staticUser = "haiteam"
    var staticPw = "haiteam"
    var selloutDb = "KOPO_PRODUCT_VOLUME"


    ///////////////////////////////////////////////////////////////




    ////////////// DB 에 저장 하기 ///////////////////////

    //    db 에 접속 할 때 처럼 접속정보 설정
    var outputUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var outputUser = "sung_2"
    var outputPw = "sung_2"

    // 저장
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password",outputPw)
    var table = "kopo_batch_season_mpara"
    //append
    selloutDataFromPg.write.mode("overwrite").jdbc(outputUrl, table, prop)











  }







}

package com.sung

import org.apache.spark.sql.SparkSession

object FunctionCollection {

  /////////////////spark 연동 /////////////////////////
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("hbProject").
        config("spark.master", "local").
        getOrCreate()

    //////////////////////// 함수 모음 ////////////////////////




    ////////////// TEST 변수들 ////////////////////////////
    var TestList = List(123,456,789,321,654,987)

    var TestArray = Array(741,852,963,369,258,147)

    /////////////////////////////////////////////////////



    // 함수화 하기 //

    //  함수이름         (변수들어오는곳:타입) : 리턴될때 변수타입
    def discountedPrice(price:Double, rate:Double) :Double = {
      /* 함수 실행 코드 */
      var discount = price * rate
      var returnValue = price - discount
      //      값
      returnValue
    }
    //    함수 사용하기
    var orgrRate = 0.2
    var orgPrice = 2000
    var newrPrice =
    //      함수이름        (넣어줄 변수)
      discountedPrice(orgPrice,orgrRate)


    /////////////////////////////////////////////////////////////////////////



    ///// Sliding /////

    //  분할 해준다

    // 슬라이딩 안에 들어간 숫자는 3개씩 두칸을 넘어가며 나누겠다
    // 뒷 숫자를 입력 하지 않으면 1칸씩 넘어가며 나눈다

    //TestListSlide: List[List[Int]] = List(List(123, 456, 789), List(789, 321, 654), List(654, 987))

    var TestListSlide = TestList.sliding(3,2).toList

    //////////////////


    /////// SPARK.SQL ////

//    SPARK에서 SQL문 사용하기
//    따옴표 안에 SQL문을 작성해 주면 된다

    var data1 = spark.sql("select regionid, productgroup, yearweek, cast(qty as double)" +
      "from maindata")
//    from 에서는 데이터 프레임 의 테이블이 아닌 TempView 테이블을 입력해 준다
  }









}

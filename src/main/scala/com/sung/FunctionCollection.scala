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



    ///// Sliding /////

    //  분할 해준다

    // 슬라이딩 안에 들어간 숫자는 3개씩 두칸을 넘어가며 나누겠다
    // 뒷 숫자를 입력 하지 않으면 1칸씩 넘어가며 나눈다

    //TestListSlide: List[List[Int]] = List(List(123, 456, 789), List(789, 321, 654), List(654, 987))

    var TestListSlide = TestList.sliding(3,2).toList

    //////////////////


  }









}

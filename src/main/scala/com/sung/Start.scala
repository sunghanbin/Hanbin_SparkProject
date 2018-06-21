package com.sung
////////////// spark 연동 라이브러리 //////////
import org.apache.spark.sql.SparkSession

object Start {
  /////////////////spark 연동 /////////////////////////
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("hbProject").
        config("spark.master", "local").
        getOrCreate()

    /////////////// spark-shell 기본 명령어(interactive Mode)//////////////////////
    /* :help 제공하는 명령어 목록
       :history 현재까지 사용했던 변수목록
       변수명.tab 사용할수 있는 명령어 목록
       ctrl + D 종료*/
    ////////////////////////////////////////////////////////////////////////////

//    intellij 단축키

//    범위 주석
//    ctrl + shift + /  ==>  /**/


////////////////////////////////////////


    //// Scala 변수선언 ////

    // 메인데이터, 상수값, 한번선언 변경 불가

    val maindata = 123

    // 일반변수 선언

    var subdata = 456

    //////////////////////////////


    //// Scala 자료형 ////

    // 수(Number) //

    // Spark 에서는 자바처럼 따로 변수타입을 정해주지 않아도 알아서 정해준다.

    // int 타입

    var int1 = 10

    // double 타입

    var double1 = 10.0

    // 정수 뒤에 d 를 붙이면 실수 로 나온다.

    var double2 = 10d

    // 변수에 type 확인 하려 할때(getClass)

    double1.getClass

    ////////////////

    // 문자열(String) //

    // 생성
    var name = "sung'hanbin'"


    // 문자열 연산

    var dunkin = "coffee"
    var donuts = "&donuts"

    // 문자열 합치기

    var dunkindonuts = dunkin+donuts

    // 원하는 문자 뽑아내기

     dunkin = dunkindonuts.substring(0,2)
     donuts = dunkindonuts.substring(5,8)

    // 특정 구분자(delimeter) 활용하여 필요한 내용 추출

    var myhometown = "seoul;Dobong_gu;Dobong_1_dong"
    var myhometownSet = myhometown.split(";")

    // 문자열 길이 구하기

    myhometown.length()

    // 특정 문자열 변경 및 제외

    myhometown = myhometown.replace(";","%")

    myhometown = myhometown.replace("%","")

    // 타입 변환

    // 숫자 -> 문자열

    var intStr = int1.toString

    // 문자열 -> 숫자

    var intinting = intStr.toDouble


    //// 리스트 (어레이보다 리스트가 속도 빠름).. 삭제와 추가 가능 하나 기존내용을 변경 하는 update 는 불가능////

    // 빈리스트 생성하기

    var binlist = List.empty

    // 리스트 생성

    var momolist = List(1,10,100,1000,10000)

    // 리스트 크기 보기

    var listSize = momolist.size

    // 리스트 연산 최대값,최소값

    var maxMomo = momolist.max

    var minMOmo = momolist.min

    // 리스트 추가

    momolist ++= List(100000)

    // 리스트 삭제

    // filter 함수 true 는 통과 하여 남는다
    var momolist1 = momolist.filter(x=>{x!=minMOmo})

    //리스트 정렬

    // 오름차순

    momolist = momolist.sortBy(x=>{x})

    // 내림차순  **음수는 작을수록 큰수 이므로....

    momolist = momolist.sortBy(x=>{-x})

    /////////////////////////////////////////////


    //// 배열(Array), 어레이 는 업데이트 가능 하다 ////

    // 빈 어레이 생성

    var binArray = Array.empty

    // Array 생성

    var numdArray = Array(3,30,300,3000,30000)

    // 배열 크기,연산,추가,삭제,정렬 list 와 동일


    // 배열 업데이트

    //앞에 입력하는 숫자는 Array 의 인덱스 를 가르킨다

    numdArray.update(3,300000)


    /////////////////////////////////////////////

    ///// DataFrame ==> RDD 로 변경 /////////

    TestData.rdd


    //// 수학 함수 math ////

    // math.pow() //

    // 제곱 연산

    var pow1 = math.pow(int1,double1)

    // math.round() //

    // 소숫점 반올림

    var sell1 = math.round(math.pow(int1,double1))


    //    while 문 //

    //    조건문의 조건이 참인경우 루프 거짓일 경우 탈출


    //  문제 //
    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size
    var i = 0

    while(i < priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
      i=i+1// 스칼라에서는 전위증가 후위증가 지원안함
    }


////////////////////////////////////////////////////////////////////////




    // for 문 //
//    다른 언어와 조금 다르다 자세한 설명은 one note

    var priceData1 = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate1 = 0.2
    var priceDataSize1 = priceData1.size

    /*  priceData1 의 어레이 사이즈는 5이다  to 를 사용하게 되면 0부터 증가 하기 때문에 6되기 때문에
        until 을 사용하여 0~4 까지 5번 값을 대입 한다  */
    for(i <-0 until priceDataSize1){
      var promotionEffect = priceData1(i) * promotionRate1
      priceData(i) = priceData(i) - promotionEffect
    }

////////////////////////////////////////////////////////////////////////



/////////////////// TempView(임시테이블) //////////////////////////////


//    임시테이블 생성
//   데이터프레임.               ("임시테이블 명")
    selloutData.createTempView("maindata")

//    임시테이블 Drop
//                            ("삭제할 임시테이블")
    spark.catalog.dropTempView("maindata")

//    임시 테이블을 보려면 변수에 spark.sql함수를 사용해  조회문을 이용하여 담아서 볼수있다!!

//////////////////////////////////////////////////////////////////////



//////////////////////////////////////////////////////////////////////




///////////////// 데이터 프레임 다루기 ///////////////////////////////////

//  데이터프레임명.schema
    selloutData.schema

//    컬럼인덱스 생성
//    .indexsOf("")
//    WHy? 컬럼위치가 변경되더라도 위치에 대한 정보를 계속 유지하기 위함

//                    = 데이터의 컬럼을 변수에 담는다
    var indexsColumns = TestData2.columns


//  인덱스 컬럼 이름 = 컬럼을담은변수.인덱스 생성 해주는 명령어(기존 컬럼 이름)
    var RegionidNo = indexsColumns.indexOf("REGIONID") // 0번 인덱스가 되는것이다
    var ProductgroupNo = indexsColumns.indexOf("PRODUCTGROUP")
    var YearweekNo = indexsColumns.indexOf("YEARWEEK")
    var QtyNo = indexsColumns.indexOf("QTY")



////////////////////////////////////////////////////////////////////////


///////////////////// RDD다루기 /////////////////////////////////////////

//    DataFrame ==> RDD 변환
//    .rdd

//            = RDD로 변환할 데이터프레임.rdd
    var RddGo = indexsColumns.rdd




/////// RDD 정제연산 ///////

// 코드 분석


/* RddGo.filter RddGo 에 있는 정보를  x에 담아 밀어 넣는다*/
    var filterexRdd = TestData2.filter(x=> {
      // 데이터 한줄씩 들어옴 => [A01,PRODUCT62,201632,16827.0000000000]

      var checkValid = true

      var yearweek = x.getString(yearweekNo)
//                 = yearweekNo 에 있는 값을  x.getString 문자열로 가져오겠다
      //                                    x.Double 숫자로

      if(yearweek.length != 6){checkValid = false}
      checkValid              // if 문을 타고 들어온 값은  삭제 한다  chekValid = false
    })


//    디버깅 하기
    /*기존 데이터 를 유지하기 위해 테스트 용 데이터를 변수 에 담아준다*/
    var deBugRdd = BasicRdd

    /*하나의 값을  x 변수에 담아  준다*/
    var deBugRdd2 = deBugRdd.first

    /*디버그 해볼 코드를 실행 한다 */

    var yearweek = x.getString(yearweekNo)








    ////////////////////////////////














  }











}

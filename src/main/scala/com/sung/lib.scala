package com.sung

import org.apache.spark.sql.SparkSession

object lib {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("hbProject").
        config("spark.master", "local").
        getOrCreate()

//    날짜 라이브러리 //
    import java.util.Calendar

//    라이브러리 사용하기


//    그레고리력? 율리우스력?
    var calendar = Calendar.getInstance()

//    시스템 년시분초
    var time = calendar.getTime()
//    time: java.util.Date = Tue Jun 19 20:22:59 KST 2018

//    시스템 시간
    var hour = time.getHours()
//    hour: Int = 20

//    시스템 분
    var minutes = time.getMinutes()
//    minutes: Int = 22


//     문제 //

    var year = Array(2000,2001,2002,2003,2004,2005,2006,2007,2008)

    var year1 = year.filter(x=>{x == 2008-3 })


///////////////////////////////////////////////////////////////////////////////
















  }

}

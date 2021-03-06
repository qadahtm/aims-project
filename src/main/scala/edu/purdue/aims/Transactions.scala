package edu.purdue.aims

import java.util.concurrent.ExecutorService
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalikejdbc._
import spray.json.JsNumber
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsArray
import spray.json.JsBoolean
import java.util.concurrent.Executors

object TxnUtils {

  val reinitbal = 10000
  val r = Randomdata.syntax("r")

  val READ_OP = 1
  val UPDATE_OP = 3

  val default_regex = "[A-C]"

  val epool = Executors.newFixedThreadPool(5);

  var ddelay = 200L; //millisecconds
  var mworkload = false;
  var mcontrol: Set[Int] = Set[Int]()
  var mprob = 0.1f // default probability 10%
  val mtrans = ListBuffer[Long]()
  var maxm: Long = 0

  def setDDelay(_ddelay: Long) = {
    ddelay = _ddelay
  }

  def setMWorkload(is: Boolean) = {
    mworkload = is
  }

  def setMMax(max: Long) = {
    maxm = max
  }

  def setMControl(that: Set[Int]) = {
    mcontrol = that
  }

  def getMTxCount() = mtrans.size

  val mdist = Array.ofDim[Boolean](100)

  def setMProb(p: Float) = {
    assert(p > 0.0 && p < 1.0)
    mprob = p
    val pm = (p * 100).toInt

    for (i <- (0 to pm - 1)) {
      mdist(i) = true
    }
  }

  def runConvWorkload(countryLabels: List[String]) = {
    // 200 tx instances
    // 10 x A-> B

    val r = Randomdata.syntax("r")
    //    val nn = (1 to n / 10)

    val dist = List(0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 6, 6)
    //    val pw = new PrintWriter(new File("wl.trace"))

    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia", "Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")

    val res = ListBuffer[List[Long]]()

    runTransactionClassF("fname", "fname", "[A-D]", "[D-E]")

    runTransactionClassF("fname", "fname", "[D-F]", "[F-H]")

    runTransactionClassF("fname", "fname", "[F-I]", "[I-K]")

    runTransactionClassF("fname", "fname", "[I-L]", "[L-N]")

    runTransactionClassF("fname", "fname", "[L-O]", "[O-R]")

    runTransactionClassF("fname", "fname", "[O-V]", "[V-Z]")

    runTransactionClassF("fname", "lname", "[V-Z]", "[A-C]")

    runTransactionClassF("lname", "lname", "[A-D]", "[D-E]")

    runTransactionClassF("lname", "lname", "[D-F]", "[F-H]")

    runTransactionClassF("lname", "lname", "[F-I]", "[I-K]")

    runTransactionClassF("lname", "lname", "[I-L]", "[L-N]")

    runTransactionClassF("lname", "lname", "[L-O]", "[O-R]")

    runTransactionClassF("lname", "lname", "[O-V]", "[V-Z]")

    runTransactionClassF("lname", "fname", "[V-Z]", "[A-C]")

    runTransactionClassF("lname", "fname", "[B-K]", "[R-Z]")

    runTransactionClassF("fname", "lname", "[A-G]", "[H-Z]")

    runTransactionClassF("fname", "lname", "[A-V]", "[W-Z]")

    runTransactionClassF("lname", "fname", "[A-V]", "[W-Z]")

    val distMap = List(tp1, tp2, tp3, tp4, tp5, tp6, tp7)

    for (i <- (1 to 180)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }
  }

  def getTxId(session: DBSession) = {
    sql"select txid_current()".list.result(x => {
      x.long(1)
    }, session)(0)
  }

  def accReads(data: List[Randomdata], txid: Long): (List[List[Long]], Float) = {
    val res = ListBuffer[List[Long]]()

    var acc = 0f
    for (i <- (0 to data.size - 2)) {
      res += List(System.currentTimeMillis, txid, data(i).id, 1)
      acc += data(i).bankbalance * (1 / (data.size - 1))
    }

    return (res.toList, acc)
  }

  def runTransactionClassF(attrName: String, nattrName: String, regex: String, nregex: String) = {

    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L
    println(s"attrName = ${attrName}, regex = ${regex}")
    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {

        txid = getTxId(session);
        var data: List[Randomdata] = List[Randomdata]()
        var acc = 0f
        if (attrName.equalsIgnoreCase("fname")) {
          data = sql"select * from randomdata where fname ~ ${regex}".map(Randomdata(r)).list().apply()
        } else if (attrName.equalsIgnoreCase("lname")) {
          data = sql"select * from randomdata where lname ~ ${regex}".map(Randomdata(r)).list().apply()
        }

        if (data.size > 0) {
          val (logreads, tacc) = accReads(data, txid)
          acc = tacc
          res ++= logreads
        }

        var tdata: List[Randomdata] = List[Randomdata]()

        if (nattrName.equalsIgnoreCase("fname")) {
          tdata = sql"select * from randomdata where fname ~ ${nregex} limit 10".map(Randomdata(r)).list().apply()
        } else if (attrName.equalsIgnoreCase("lname")) {
          tdata = sql"select * from randomdata where lname ~ ${nregex} limit 10".map(Randomdata(r)).list().apply()
        }

        tdata.foreach { to =>
          {
            val nbal = acc + to.bankbalance
            val id1 = to.id

            sql"update Randomdata set bankbalance = ${nbal} where id = ${id1}".update.apply()
            res += List(System.currentTimeMillis(), txid, to.id, UPDATE_OP)

          }
        }

      }
    }
    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))

  }

  def runConfWorkloadTry2(countryLabels: List[String], n1: Int, n2: Int) = {
    var ci = 0
    var ci2 = 0
    var totaltxn = 0

    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to n1)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        totaltxn = totaltxn + 1
      }
      // pick a destination country at random.  
      do {
        ci2 = scala.util.Random.nextInt(countryLabels.size)
      } while (ci == ci2)

      for (j <- (1 to n2)) {
        runClassBTransaction(ci, ci2, "[A-C]", countryLabels)
        totaltxn = totaltxn + 1
      }

      ci = ci + 1
    }

    println("Total number of txn instances executed = " + totaltxn)
  }

  def runConfWorkloadTry3(countryLabels: List[String], n1: Int, n2: Int, n3: Int, n4: Int) = {
    var ci = 0
    var ci2 = 0
    var totaltxn = 0

    // allowed inter-country transactions (by indexes)
    val intercmap = Map(0 -> (1, "[A-C]"),
      1 -> (2, "[A-C]"),
      2 -> (3, "[A-C]"),
      3 -> (4, "[A-C]"),
      4 -> (0, "[A-C]"))

    val fanmax = 10
    val fanmin = 5

    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to n1)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        totaltxn = totaltxn + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to n2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        totaltxn = totaltxn + 1
      }

      for (j <- (1 to n3)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        totaltxn = totaltxn + 1
      }

      for (j <- (1 to n4)) {
        val rfan = scala.util.Random.nextInt(31) + 20
        val scis = List.fromArray(Array.fill(rfan)(ci))
        val dci = ci
        runClassDTransaction2(scis, dci, countryLabels)
        totaltxn = totaltxn + 1
      }

      ci = ci + 1
    }

    println("Total number of txn instances executed = " + totaltxn)
  }

  def runConfWorkloadTry4(countryLabels: List[String]) = {
    var ci = 0
    var ci2 = 0
    var totaltxn = 0
    var gtxncount = 0
    val maxtxncount = 300
    
    // allowed inter-country transactions (by indexes)
    val intercmap = Map(
      0 -> (1, "[A-C]"),
      1 -> (2, "[A-C]"),
      2 -> (3, "[A-C]"),
      3 -> (4, "[A-C]"),
      4 -> (0, "[A-C]"))

    // do attack in one IB
    // run 50 normal transactions

    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        totaltxn = totaltxn + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        totaltxn = totaltxn + 1
      }

      for (j <- (1 to 4)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        totaltxn = totaltxn + 1
      }

      for (j <- (1 to 2)) {
        val rfan = scala.util.Random.nextInt(31) + 20
        val scis = List.fromArray(Array.fill(rfan)(ci))
        val dci = ci
        runClassDTransaction2(scis, dci, countryLabels)
        totaltxn = totaltxn + 1
      }

      ci = ci + 1
    }
    println(s"Executed ${totaltxn} on all IBs")
    

    // attack IB 0
    ci = 0
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    var tmp = 0
    // execute normal 
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 4)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 2)) {
        val rfan = scala.util.Random.nextInt(31) + 20
        val scis = List.fromArray(Array.fill(rfan)(ci))
        val dci = ci
        runClassDTransaction2(scis, dci, countryLabels)
        tmp = tmp + 1
      }

      ci = ci + 1
    }
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    
    // attack IB 0 twice
    ci = 0
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    tmp = 0
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 4)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 2)) {
        val rfan = scala.util.Random.nextInt(31) + 20
        val scis = List.fromArray(Array.fill(rfan)(ci))
        val dci = ci
        runClassDTransaction2(scis, dci, countryLabels)
        tmp = tmp + 1
      }

      ci = ci + 1
    }
    
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    
    tmp = 0
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
     
      for (j <- (1 to 8)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }
     
      ci = ci + 1
    }
    
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    ci = 0
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    if (totaltxn < maxtxncount){
      // run high fanout on IB 0
      val txndiff = maxtxncount-totaltxn
      println(s"remaining txn quota is ${txndiff}")
      
      val ccount = totaltxn
      ci = 0
      tmp = 0
      for (j <- (ccount to maxtxncount-1)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }
      
       println(s"Executed ${tmp} on IB ${ci}")    
       totaltxn = totaltxn + tmp      
    }
    
    
    println("Total number of txn instances executed = " + totaltxn)
  }
  
  def runConfWorkloadTry5(countryLabels: List[String]) = {
    var ci = 0
    var ci2 = 0
    var totaltxn = 0
    var gtxncount = 0
    val maxtxncount = 300
    
    // allowed inter-country transactions (by indexes)
    val intercmap = Map(
      0 -> (1, "[A-C]"),
      1 -> (2, "[A-C]"),
      2 -> (3, "[A-C]"),
      3 -> (4, "[A-C]"),
      4 -> (0, "[A-C]"))

    // do attack in one IB
    // run 50 normal transactions

    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        totaltxn = totaltxn + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        totaltxn = totaltxn + 1
      }

      for (j <- (1 to 6)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        totaltxn = totaltxn + 1
      }
      ci = ci + 1
    }
    println(s"Executed ${totaltxn} on all IBs")
    

    // attack IB 0
    ci = 0
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    var tmp = 0
    // execute normal 
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 6)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }
      ci = ci + 1
    }
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    
    ci = 0
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    ci = 4
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    tmp = 0
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
      val pair = intercmap(ci)
      ci2 = pair._1
      val fnamepred = pair._2
      for (j <- (1 to 2)) {
        runClassBTransaction(ci, ci2, fnamepred, countryLabels)
        tmp = tmp + 1
      }

      for (j <- (1 to 6)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }

      ci = ci + 1
    }
    
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    
    tmp = 0
    for (i <- (1 to countryLabels.size)) {
      if (ci == countryLabels.size) ci = 0

      val country1 = countryLabels(ci)
      for (j <- (1 to 2)) {
        runClassATransaction(countryLabels.indexOf(country1), countryLabels)
        tmp = tmp + 1
      }
     
      for (j <- (1 to 8)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }
     
      ci = ci + 1
    }
    
    println(s"Executed ${tmp} on all IBs")    
    totaltxn = totaltxn + tmp
    
    // attack IB4
    ci = 4
    gtxncount = runConnectedTransactionGroup1(ci, countryLabels)
    println(s"dependent transaction count = ${gtxncount-1}")
    totaltxn = totaltxn + gtxncount
    
    
    if (totaltxn < maxtxncount){
      // run high fanout on IB 0
      val txndiff = maxtxncount-totaltxn
      println(s"remaining txn quota is ${txndiff}")
      
      val ccount = totaltxn
      ci = 0
      tmp = 0
      for (j <- (ccount to maxtxncount-1)) {
        val sci = ci
        val rfan = scala.util.Random.nextInt(16) + 15
        val dcis = List.fromArray(Array.fill(rfan)(ci))
        runClassCTransaction2(sci, dcis, countryLabels)
        tmp = tmp + 1
      }
      
       println(s"Executed ${tmp} on IB ${ci}")    
       totaltxn = totaltxn + tmp      
    }
    
    
    println("Total number of txn instances executed = " + totaltxn)
  }

  def runConvWorkloadEC1(countryLabels: List[String]) = {
    // 200 tx instances
    // 10 x A-> B

    val r = Randomdata.syntax("r")
    //    val nn = (1 to n / 10)

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    val dist = List(0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 6, 6)
    //    val pw = new PrintWriter(new File("wl.trace"))

    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia", "Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")

    val res = ListBuffer[List[Long]]()

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        val data = sql"select * from randomdata".map(Randomdata(r)).list().apply()

        var acc = 0f
        for (i <- (0 to data.size - 2)) {
          res += List(System.currentTimeMillis, txid, data(i).id, 1)
          acc += data(i).bankbalance * (1 / (data.size - 1))
        }

        val to = data(data.size - 1)
        val nbal = acc + to.bankbalance
        val id1 = to.id

        sql"update Randomdata set bankbalance = ${nbal} where id = ${id1}".update.apply()
        val we = List(ctime, txid, to.id, 3)
        res += we
      }
    }
    ctime = System.currentTimeMillis()

    res.foreach(AIMSLogger.logTxAccessEntry(_))
    AIMSLogger.logResponseTime(txid, stime, ctime)

    val distMap = List(tp1, tp2, tp3, tp4, tp5, tp6, tp7)

    for (i <- (1 to 199)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }
  }

  def runMWorkload(n: Int, ddelay: Long, mdist: List[Boolean], countryLabels: List[String], epool: ExecutorService) {
    //    println(n)
    //    println(dist.toList)
    val maxm = mdist.map { x => { if (x) 1 else 0 } }.reduce(_ + _)

    val dist = List(0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 6, 6)
    //    val pw = new PrintWriter(new File("wl.trace"))

    val mtrans = ListBuffer[Long]()

    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia", "Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")

    val distMap = List(tp1, tp2, tp3, tp4, tp5, tp6, tp7)
    val nn = n / 10

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          // flip a biased coin to determine if the transaction is malicious
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10, countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10, countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn + 1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2, "[A-C]", countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn + 1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2, "[A-C]", countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))) {
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1), ddelay))
          }
        }
      }
    }

    println(s"Total number of transaction is $n, and malicious ${mtrans.size}")

  }

  def runNWorkload(n: Int, countryLabels: List[String]) {

    val dist = List(0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 5, 6, 6)
    //    val pw = new PrintWriter(new File("wl.trace"))

    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia", "Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")

    val distMap = List(tp1, tp2, tp3, tp4, tp5, tp6, tp7)
    val nn = n / 10

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1), countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10, countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10, countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn + 1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2, "[A-C]", countryLabels)
    }

    for (i <- (1 to nn)) {
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn + 1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2, "[A-C]", countryLabels)
    }

  }

  // runs a workload according the given distribution
  def runWorkloadDist(dist: List[Int], n: Int) {

  }

  // class A: run a transfer between two random accounts within a given country

  def runClassATransaction(in_ci: Int, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    var ci = in_ci

    stime = System.currentTimeMillis()

    val sqlBuffer = ListBuffer[JsObject]()

    DB localTx { implicit session =>
      {

        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        var from = List[Randomdata]()

        val fromsql = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, countryLabels(ci))
            .append(sqls"order by RANDOM()")
            .limit(1)
        }

        do {
          if (ci == countryLabels.size) ci = 0
          from = withSQL {
            select
              .from(Randomdata as r)
              .where.eq(r.country, countryLabels(ci))
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()
          if (from.size == 0) ci = ci + 1
        } while (from.size == 0)

        sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))
        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, ci)

        val to = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, countryLabels(ci))
            .append(sqls"order by RANDOM()")
            .limit(1)
        }.map(Randomdata(r)).list.apply()

        val amnt = (from(0).bankbalance * 0.1)
        var snbal = from(0).bankbalance - amnt

        if (snbal <= 0) snbal = Random.nextFloat() * reinitbal

        val dnbal = (from(0).bankbalance * 0.1) + to(0).bankbalance
        val sid = from(0).id
        val did = to(0).id

        sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
        res += List(System.currentTimeMillis, txid, sid, UPDATE_OP, ci)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
          "obal" -> JsNumber(from(0).bankbalance),
          "nbal" -> JsNumber(snbal))

        sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
        res += List(System.currentTimeMillis(), txid, did, UPDATE_OP, ci)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to(0).id),
          "obal" -> JsNumber(to(0).bankbalance),
          "nbal" -> JsNumber(dnbal))
      }
    }
    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)

    // flip a biased coin to determine if the transaction is malicious
    //    if (mworkload && mtrans.size < maxm  && mdist(Random.nextInt(mdist.size))) {
    //      
    //      mtrans += txid
    //      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
    //      "class" -> JsString("A"), "ci" -> JsNumber(ci), 
    //      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
    //      AIMSLogger.logTransaction(txspec)
    //      epool.execute(new IDSAlert(txid, ddelay))
    //    }
    //    else {
    val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
      "class" -> JsString("A"), "ci" -> JsNumber(ci), "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
    AIMSLogger.logTransaction(txspec)
    //    }

    res.foreach { AIMSLogger.logTxAccessEntry(_) }

    return res.toList
  }

  def runClassATransactionMD(mtxn: Boolean, in_ci: Int, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    var ci = in_ci

    stime = System.currentTimeMillis()

    val sqlBuffer = ListBuffer[JsObject]()

    DB localTx { implicit session =>
      {

        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        var from = List[Randomdata]()

        val fromsql = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, countryLabels(ci))
            .append(sqls"order by RANDOM()")
            .limit(1)
        }

        do {
          if (ci == countryLabels.size) ci = 0
          from = withSQL {
            select
              .from(Randomdata as r)
              .where.eq(r.country, countryLabels(ci))
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()
          if (from.size == 0) ci = ci + 1
        } while (from.size == 0)

        sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))
        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, ci)

        val to = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, countryLabels(ci))
            .append(sqls"order by RANDOM()")
            .limit(1)
        }.map(Randomdata(r)).list.apply()

        val amnt = (from(0).bankbalance * 0.1)
        var snbal = from(0).bankbalance - amnt

        if (snbal <= 0) snbal = Random.nextFloat() * reinitbal

        val dnbal = (from(0).bankbalance * 0.1) + to(0).bankbalance
        val sid = from(0).id
        val did = to(0).id

        sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
        res += List(System.currentTimeMillis, txid, sid, UPDATE_OP, ci)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
          "obal" -> JsNumber(from(0).bankbalance),
          "nbal" -> JsNumber(snbal))

        sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
        res += List(System.currentTimeMillis(), txid, did, UPDATE_OP, ci)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to(0).id),
          "obal" -> JsNumber(to(0).bankbalance),
          "nbal" -> JsNumber(dnbal))
      }
    }
    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)

    if (mtxn) {

      mtrans += txid
      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
        "class" -> JsString("A"), "ci" -> JsNumber(ci),
        "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
      AIMSLogger.logTransaction(txspec)
      epool.execute(new IDSAlert(txid, ddelay))
    } else {
      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
        "class" -> JsString("A"), "ci" -> JsNumber(ci), "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
      AIMSLogger.logTransaction(txspec)
    }

    res.foreach { AIMSLogger.logTxAccessEntry(_) }

    return res.toList
  }

  // class B: run a transfer between two random accounts from two different countries. 
  def runClassBTransaction(_sci: Int, _dci: Int, dregex: String, countryLabels: List[String]): List[List[Long]] = {
    runClassBTransaction(countryLabels(_sci), countryLabels(_dci), dregex, countryLabels)
  }

  def runClassBTransaction(_scountry: String, _dcountry: String, dregex: String, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L

    val sqlBuffer = ListBuffer[JsObject]()

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        val from = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, _scountry)
            .append(sqls"order by RANDOM()")
            .limit(1)
        }.map(Randomdata(r)).list.apply()

        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, countryLabels.indexOf(_scountry), countryLabels.indexOf(_dcountry))
        sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))

        val to = sql"select * from randomdata where country = ${_dcountry} and fname ~ ${dregex} order by RANDOM() limit 1".map(Randomdata(r)).list().apply()
        val amnt = (from(0).bankbalance * 0.1)
        var snbal = from(0).bankbalance - amnt

        if (snbal <= 0) snbal = Random.nextFloat() * reinitbal

        val dnbal = (from(0).bankbalance * 0.1) + to(0).bankbalance
        val sid = from(0).id
        val did = to(0).id

        sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
        res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP, countryLabels.indexOf(_scountry), countryLabels.indexOf(_dcountry))
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
          "obal" -> JsNumber(from(0).bankbalance),
          "nbal" -> JsNumber(snbal))

        sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
        res += List(System.currentTimeMillis, txid, to(0).id, UPDATE_OP, countryLabels.indexOf(_scountry), countryLabels.indexOf(_dcountry))
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to(0).id),
          "obal" -> JsNumber(to(0).bankbalance),
          "nbal" -> JsNumber(dnbal))
      }
    }

    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))
    // flip a biased coin to determine if the transaction is malicious
    //    if (mworkload && mtrans.size < maxm  && mdist(Random.nextInt(mdist.size))) {
    //      mtrans += txid
    //      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
    //      "class" -> JsString("B"), "sci" -> JsNumber(countryLabels.indexOf(_scountry)), 
    //      "dci" -> JsNumber(countryLabels.indexOf(_dcountry)), 
    //      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
    //      AIMSLogger.logTransaction(txspec)
    //      epool.execute(new IDSAlert(txid, ddelay))
    //    }
    //    else {
    val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
      "class" -> JsString("B"), "sci" -> JsNumber(countryLabels.indexOf(_scountry)),
      "dci" -> JsNumber(countryLabels.indexOf(_dcountry)),
      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
    AIMSLogger.logTransaction(txspec)
    //    }

    return res.toList
  }

  def runClassCTransaction2(sci: Int, dcis: List[Int], countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L

    val sqlBuffer = ListBuffer[JsObject]()

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        val scountry = countryLabels(sci)

        val from = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, scountry)
            .append(sqls"order by RANDOM()")
            .limit(1)
        }.map(Randomdata(r)).list.apply()

        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, sci.toLong)
        sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))

        val amnt = (from(0).bankbalance * 0.1)
        var snbal = from(0).bankbalance - amnt
        if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
        val sid = from(0).id

        sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
        res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP, sci.toLong)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
          "obal" -> JsNumber(from(0).bankbalance),
          "nbal" -> JsNumber(snbal))

        for (dci <- dcis) {
          val dcountry = countryLabels(dci)
          val to = withSQL {
            select
              .from(Randomdata as r)
              .where.eq(r.country, dcountry)
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()

          val damnt = amnt / dcis.size

          val did = to(0).id
          sql"update Randomdata set bankbalance = ${damnt} where id = ${did}".update.apply()
          res += List(System.currentTimeMillis, txid, did, UPDATE_OP, sci.toLong, dci.toLong)
          sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to(0).id),
            "obal" -> JsNumber(to(0).bankbalance),
            "nbal" -> JsNumber(damnt))

        }
      }
    }

    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))

    // flip a biased coin to determine if the transaction is malicious
    //    if (mworkload && mtrans.size < maxm  && mdist(Random.nextInt(mdist.size))) {
    //      mtrans += txid
    //      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
    //      "class" -> JsString("C"), "ci" -> JsNumber(sci), 
    //      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
    //      AIMSLogger.logTransaction(txspec)
    //      epool.execute(new IDSAlert(txid, ddelay))
    //    }
    //    else {
    val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
      "class" -> JsString("C"), "ci" -> JsNumber(sci),
      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
    AIMSLogger.logTransaction(txspec)
    //    }
    return res.toList
  }

  def runClassDTransaction2(scis: List[Int], dci: Int, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L

    val sqlBuffer = ListBuffer[JsObject]()

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        var tamnt = 0d

        val dcountry = countryLabels(dci)

        val to = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, dcountry)
            .append(sqls"order by RANDOM()")
            .limit(1)
        }.map(Randomdata(r)).list.apply()

        for (sci <- scis) {
          val scountry = countryLabels(sci)
          val from = withSQL {
            select
              .from(Randomdata as r)
              .where.eq(r.country, scountry)
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()

          res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, sci.toLong)
          sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))

          val amnt = (from(0).bankbalance * 0.1)

          var snbal = from(0).bankbalance - amnt
          if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
          val sid = from(0).id

          sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
          res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP, sci.toLong)
          sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
            "obal" -> JsNumber(from(0).bankbalance),
            "nbal" -> JsNumber(snbal))

          var dnbal = to(0).bankbalance + amnt
          val did = to(0).id
          sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
          res += List(System.currentTimeMillis, txid, did, UPDATE_OP, sci.toLong, dci.toLong)
          sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to(0).id),
            "obal" -> JsNumber(to(0).bankbalance),
            "nbal" -> JsNumber(dnbal))
        }
      }
    }

    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))

    //    
    //    if (mworkload && mtrans.size < maxm  && mdist(Random.nextInt(mdist.size))) {
    //      mtrans += txid
    //      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
    //      "class" -> JsString("D"), "ci" -> JsNumber(scis(0)), 
    //      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
    //      AIMSLogger.logTransaction(txspec)
    //      epool.execute(new IDSAlert(txid, ddelay))      
    //    }
    //    else {
    val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
      "class" -> JsString("D"), "ci" -> JsNumber(scis(0)), // TODO: fix this. to allow inter-country        
      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
    AIMSLogger.logTransaction(txspec)
    //    }
    return res.toList
  }

  def runClassCTransactionConnected(so: Long, dcount: Int, ci: Int, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L

    val sqlBuffer = ListBuffer[JsObject]()

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)

        val scountry = countryLabels(ci)

        val from = sql"select * from randomdata where id = ${so}".map(Randomdata(r)).list.apply()
        val sci = countryLabels.indexOf(from(0).country)
        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, sci.toLong)
        sqlBuffer += JsObject("op" -> JsNumber(READ_OP), "oid" -> JsNumber(from(0).id))

        val amnt = (from(0).bankbalance * 0.1)
        var snbal = from(0).bankbalance - amnt
        if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
        val sid = from(0).id

        sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
        res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP, sci.toLong)
        sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(from(0).id),
          "obal" -> JsNumber(from(0).bankbalance),
          "nbal" -> JsNumber(snbal))
        val tos = sql"""select * from randomdata 
          where country = ${scountry} 
            and id != ${so} 
          order by RANDOM()
          limit ${dcount}
          """.map(Randomdata(r)).list.apply()

        for (to <- tos) {
          val damnt = amnt / dcount
          val did = to.id
          sql"update Randomdata set bankbalance = ${damnt} where id = ${did}".update.apply()
          res += List(System.currentTimeMillis, txid, did, UPDATE_OP, sci.toLong, sci.toLong)
          sqlBuffer += JsObject("op" -> JsNumber(UPDATE_OP), "oid" -> JsNumber(to.id),
            "obal" -> JsNumber(to.bankbalance),
            "nbal" -> JsNumber(damnt))

        }
      }
    }

    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))

    // flip a biased coin to determine if the transaction is malicious
    //    if (mworkload && mtrans.size < maxm  && mdist(Random.nextInt(mdist.size))) {
    //      mtrans += txid
    //      val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
    //      "class" -> JsString("C"), "ci" -> JsNumber(sci), 
    //      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(true), "mdelay" -> JsNumber(ddelay))
    //      AIMSLogger.logTransaction(txspec)
    //      epool.execute(new IDSAlert(txid, ddelay))
    //    }
    //    else {
    val txspec = JsObject("txid" -> JsNumber(txid), "stime" -> JsNumber(stime), "ctime" -> JsNumber(ctime),
      "class" -> JsString("C"), "ci" -> JsNumber(ci),
      "subtx" -> JsArray(sqlBuffer.toVector), "m" -> JsBoolean(false))
    AIMSLogger.logTransaction(txspec)
    //    }
    return res.toList
  }

  def runConnectedTransactionGroup1(ci: Int, countryLabels: List[String]): Int = {
    val tcs = List("A", "B", "C", "D")
    var totalTxs = 0
    // a malicisous transaction and 
    // run an A-txn
    val A_tx = runClassATransactionMD(true, ci, countryLabels)
    totalTxs = totalTxs + 1
    val A_os = A_tx.filter { x => x(3) == 3 }.map(_(2))
    assert(A_os.size == 2)
    //    println("A- this should be 2 : "+A_os.size)
    val wo = A_os.last // get the last object written to
    var rfan = scala.util.Random.nextInt(6) + 3
    val D_os = runClassCTransactionConnected(wo, rfan, ci, countryLabels).filter { x => x(3) == 3 }.map(_(2)).drop(1)
    assert((rfan) == D_os.size)
    totalTxs = totalTxs + 1
    //    println("D- this should be "+rfan*2+" : "+D_os.size)

//    var rfan2 = scala.util.Random.nextInt(D_os.size - 2) + 5
    val lv2D_os = scala.util.Random.shuffle(D_os).take(3)

    for (D_o <- D_os) {
      rfan = scala.util.Random.nextInt(6) + 3
      val lv3D_os = runClassCTransactionConnected(D_o, rfan, ci, countryLabels).filter { x => x(3) == 3 }.map(_(2)).drop(1)
      assert((rfan) == lv3D_os.size)
      totalTxs = totalTxs + 1

//      rfan2 = scala.util.Random.nextInt(D_os.size - 2) + 5
      val lv4D_os = scala.util.Random.shuffle(lv3D_os).take(3)
      for (lv4D_o <- lv4D_os) {
        rfan = scala.util.Random.nextInt(6) + 3
        val lv5D_os = runClassCTransactionConnected(D_o, rfan, ci, countryLabels).filter { x => x(3) == 3 }.map(_(2)).drop(1)
        totalTxs = totalTxs + 1
        assert((rfan) == lv5D_os.size)
      }
    }

    return totalTxs
  }

  // TODO: Many-to-Many within the same county. 
  // 
  //  def runClassM2MTransaction(ci: Int, sc: Int, dc: Int, countryLabels: List[String]): List[List[Long]] = {
  //    val res = ListBuffer[List[Long]]()
  //    var stime = 0L
  //    var ctime = 0L
  //    var txid = 0L
  //
  //    stime = System.currentTimeMillis()
  //
  //    DB localTx { implicit session =>
  //      {
  //        txid = sql"select txid_current()".list.result(x => {
  //          x.long(1)
  //        }, session)(0)
  //
  //        
  //        var tamnt = 0d
  //        
  //        // get a number of sources tuples
  //        val cstr = countryLabels(ci)
  //        
  //        val fromto = withSQL {
  //          select
  //            .from(Randomdata as r)
  //            .where.eq(r.country, cstr)
  //            .append(sqls"order by RANDOM()")
  //            .limit(sc+dc)
  //        }.map(Randomdata(r)).list.apply()
  //        
  //        val scis = 1
  //        for (sci <- scis) {
  //          val scountry = countryLabels(sci)
  //          val from = withSQL {
  //            select
  //              .from(Randomdata as r)
  //              .where.eq(r.country, scountry)
  //              .append(sqls"order by RANDOM()")
  //              .limit(1)
  //          }.map(Randomdata(r)).list.apply()
  //
  //          res += List(System.currentTimeMillis, txid, from(0).id, READ_OP, sci.toLong)
  //          
  //          val amnt = (from(0).bankbalance * 0.1)
  //          
  //          var snbal = from(0).bankbalance - amnt
  //          if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
  //          val sid = from(0).id
  //
  //          sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
  //          res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP, sci.toLong)
  //          
  //          var dnbal = to(0).bankbalance + amnt
  //          val did = to(0).id
  //          sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
  //          res += List(System.currentTimeMillis, txid, did, UPDATE_OP, sci.toLong, dci.toLong)
  //        }
  //      }
  //    }
  //
  //    ctime = System.currentTimeMillis()
  //    AIMSLogger.logResponseTime(txid, stime, ctime)
  //    res.foreach(AIMSLogger.logTxAccessEntry(_))
  //
  //    return res.toList
  //  }

  // class C: run a transfer between one random account to a random number between 2 and N where N > 5 
  // source and destination can be from different country or same country or mixed

  def runClassCTransaction(_countryList: List[String], _N: Int, countryLabels: List[String]): List[List[Long]] = {

    val res = ListBuffer[List[Long]]()
    var rN = Random.nextInt(_N)
    if (rN < 2) rN = 2

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    _countryList.size match {
      case 1 => {
        // same country

        stime = System.currentTimeMillis()

        DB localTx { implicit session =>
          {
            txid = sql"select txid_current()".list.result(x => {
              x.long(1)
            }, session)(0)

            val _country = _countryList(0)

            val from = withSQL {
              select
                .from(Randomdata as r)
                .where.eq(r.country, _country)
                .append(sqls"order by RANDOM()")
                .limit(1)
            }.map(Randomdata(r)).list.apply()

            res += List(System.currentTimeMillis, txid, from(0).id, READ_OP)

            val to = withSQL {
              select
                .from(Randomdata as r)
                .where.eq(r.country, _country)
                .append(sqls"order by RANDOM()")
                .limit(rN)
            }.map(Randomdata(r)).list.apply()

            val amnt = (from(0).bankbalance * 0.1)
            var snbal = from(0).bankbalance - amnt

            if (snbal <= 0) snbal = Random.nextFloat() * reinitbal

            val damnt = amnt / rN

            val sid = from(0).id

            sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
            res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP)

            to.foreach { tox =>
              {
                val did = tox.id
                sql"update Randomdata set bankbalance = ${damnt} where id = ${did}".update.apply()
                res += List(System.currentTimeMillis, txid, did, UPDATE_OP)
              }
            }

          }
        }

        ctime = System.currentTimeMillis()
        AIMSLogger.logResponseTime(txid, stime, ctime)
        res.foreach(AIMSLogger.logTxAccessEntry(_))
      }

      case _ => {
        // TODO
        // different countries
        // first one is source
        // rest are destinations
        if (_countryList.size > 1) {

        }
      }
    }

    return res.toList
  }

  // class D: run a transfer between multiple account of random size between 2 and N where N > 5. 
  // 

  def runClassDTransaction(_countryList: List[String], _N: Int, countryLabels: List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var rN = Random.nextInt(_N)
    if (rN < 2) rN = 2

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    _countryList.size match {
      case 1 => {
        // same country
        stime = System.currentTimeMillis()
        DB localTx { implicit session =>
          {
            txid = sql"select txid_current()".list.result(x => {
              x.long(1)
            }, session)(0)

            val _country = _countryList(0)

            val from = withSQL {
              select
                .from(Randomdata as r)
                .where.eq(r.country, _country)
                .append(sqls"order by RANDOM()")
                .limit(rN)
            }.map(Randomdata(r)).list.apply()

            from.foreach { x =>
              {
                res += List(System.currentTimeMillis, txid, x.id, READ_OP)
              }
            }

            val to = withSQL {
              select
                .from(Randomdata as r)
                .where.eq(r.country, _country)
                .append(sqls"order by RANDOM()")
                .limit(1)
            }.map(Randomdata(r)).list.apply()

            val amnt = from.map { x =>
              {
                val damnt = (x.bankbalance * 0.1)
                var snbal = x.bankbalance - damnt
                if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
                val sid = x.id
                sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
                res += List(System.currentTimeMillis, txid, sid, UPDATE_OP)
                damnt
              }
            }.reduce(_ + _)

            to.foreach { tox =>
              {
                val did = tox.id
                sql"update Randomdata set bankbalance = ${amnt} where id = ${did}".update.apply()
                res += List(System.currentTimeMillis, txid, did, UPDATE_OP)
              }
            }

          }
        }
        ctime = System.currentTimeMillis()
        AIMSLogger.logResponseTime(txid, stime, ctime)
        res.foreach { AIMSLogger.logTxAccessEntry(_) }

      }
      case _ => {
        // TODO
        // different countries
        // last country is destination 
        // rest are sources
        if (_countryList.size > 1) {

        }
      }
    }

    return res.toList
  }

  def runTxnInstance(tc_alpha1: String, tc_alpha2: String, n: Int): List[List[Long]] = {
    val r = Randomdata.syntax("r")
    val nn = (1 to n / 10)
    val res = ListBuffer[List[Long]]()

    var stime = 0L
    var ctime = 0L
    var txid = 0L

    for (i <- nn) {
      stime = System.currentTimeMillis()
      DB localTx { implicit session =>
        {

          txid = sql"select txid_current()".list.result(x => {
            x.long(1)
          }, session)(0)

          val from = withSQL {
            select
              .from(Randomdata as r)
              .where.like(r.fname, tc_alpha1 + "%")
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()

          val to = withSQL {
            select
              .from(Randomdata as r)
              .where.like(r.fname, tc_alpha2 + "%")
              .append(sqls"order by RANDOM()")
              .limit(1)
          }.map(Randomdata(r)).list.apply()

          val nbal = (from(0).bankbalance * 0.1) + to(0).bankbalance
          val id1 = to(0).id

          sql"update Randomdata set bankbalance = ${nbal} where id = ${id1}".update.apply()
          val re = List(System.currentTimeMillis, txid, from(0).id, 1)
          val we = List(System.currentTimeMillis, txid, to(0).id, 3)
          res += re
          res += we
        }
      }

      ctime = System.currentTimeMillis()
      AIMSLogger.logResponseTime(txid, stime, ctime)
    }

    res.foreach(AIMSLogger.logTxAccessEntry(_))

    return res.toList
  }
}
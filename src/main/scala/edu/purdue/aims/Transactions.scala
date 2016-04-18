package edu.purdue.aims

import scala.collection.mutable.ListBuffer
import scalikejdbc._
import scala.util.Random
import java.io.PrintWriter
import java.io.File
import java.util.concurrent.ExecutorService

object TxnUtils {

  val reinitbal = 10000
  val r = Randomdata.syntax("r")

  val READ_OP = 1
  val UPDATE_OP = 3
  
  
  def runConvWorkload(countryLabels:List[String]) = {
    // 200 tx instances
    // 10 x A-> B
    
    val r = Randomdata.syntax("r")
//    val nn = (1 to n / 10)
    
    val dist = List(0,0,0,0, 1,1,1,1,1, 2,2, 3,3, 4, 5, 6,6)
//    val pw = new PrintWriter(new File("wl.trace"))
    
    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia","Philippines", "Thailand", "Malaysia")
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
    
    runTransactionClassF("fname", "lname" , "[V-Z]", "[A-C]")
    
    runTransactionClassF("lname", "lname", "[A-D]", "[D-E]")
    
    runTransactionClassF("lname", "lname", "[D-F]", "[F-H]")
    
    runTransactionClassF("lname", "lname", "[F-I]", "[I-K]")
    
    runTransactionClassF("lname", "lname", "[I-L]","[L-N]")
    
    runTransactionClassF("lname", "lname", "[L-O]", "[O-R]")
    
    runTransactionClassF("lname", "lname", "[O-V]", "[V-Z]")
    
    runTransactionClassF("lname", "fname",  "[V-Z]", "[A-C]")
    
    runTransactionClassF("lname", "fname",  "[B-K]", "[R-Z]")
    
    runTransactionClassF("fname", "lname",  "[A-G]", "[H-Z]")
    
    runTransactionClassF("fname", "lname",  "[A-V]", "[W-Z]")
    
    runTransactionClassF("lname", "fname",  "[A-V]", "[W-Z]")
    
    val distMap = List(tp1,tp2,tp3,tp4,tp5,tp6,tp7)
    
    for (i <- (1 to 180)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }    
  }
  
  def getTxId(session: DBSession) = {
    sql"select txid_current()".list.result(x => {
            x.long(1)
          }, session)(0)
  }
  
  def accReads(data:List[Randomdata], txid:Long): (List[List[Long]], Float) = {
    val res = ListBuffer[List[Long]]()
    
    var acc = 0f
    for (i <- (0 to data.size-2)){
      res += List(System.currentTimeMillis, txid, data(i).id, 1)
      acc += data(i).bankbalance*(1/(data.size-1))
    }
    
    return (res.toList, acc)
  }
  
  def runTransactionClassF(attrName:String, nattrName:String, regex:String, nregex:String) = {
    
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L
    println(s"attrName = ${attrName}, regex = ${regex}")
    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
        {
          
          txid = getTxId(session);
          var data:List[Randomdata] = List[Randomdata]()
          var acc = 0f
          if (attrName.equalsIgnoreCase("fname")){
            data = sql"select * from randomdata where fname ~ ${regex}".map(Randomdata(r)).list().apply()
          }
          else if (attrName.equalsIgnoreCase("lname")){
            data = sql"select * from randomdata where lname ~ ${regex}".map(Randomdata(r)).list().apply()
          }
          
          if (data.size > 0){
            val (logreads, tacc) = accReads(data,txid)
            acc = tacc
            res ++= logreads            
          }                 
          
          var tdata:List[Randomdata] = List[Randomdata]()
          
          if (nattrName.equalsIgnoreCase("fname")){
            tdata = sql"select * from randomdata where fname ~ ${nregex} limit 10".map(Randomdata(r)).list().apply()
          }
          else if (attrName.equalsIgnoreCase("lname")){
            tdata = sql"select * from randomdata where lname ~ ${nregex} limit 10".map(Randomdata(r)).list().apply()
          }
          
          tdata.foreach { to => {
            val nbal = acc + to.bankbalance
            val id1 = to.id
  
            sql"update Randomdata set bankbalance = ${nbal} where id = ${id1}".update.apply()
            res += List(System.currentTimeMillis(), txid, to.id, UPDATE_OP)
            
          } }             
          
        }
      }
    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime , ctime)
    res.foreach(AIMSLogger.logTxAccessEntry(_))
    
  }
  
  def runConfWorkloadTry2(countryLabels:List[String]) = {
    var ci = 0
    
    for (i <- (1 to 200)){
      if (ci == countryLabels.size) ci = 0
      
      val country1 = countryLabels(ci)
      for (j <- (1 to 100)){
        runClassATransaction(countryLabels.indexOf(country1),countryLabels)  
      }
      
      ci = ci + 1
    }
  }
  
  
  def runConvWorkloadEC1(countryLabels:List[String]) = {
    // 200 tx instances
    // 10 x A-> B
    
    val r = Randomdata.syntax("r")
//    val nn = (1 to n / 10)
    
    var stime = 0L
    var ctime = 0L
    var txid = 0L
    
    val dist = List(0,0,0,0, 1,1,1,1,1, 2,2, 3,3, 4, 5, 6,6)
//    val pw = new PrintWriter(new File("wl.trace"))
    
    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia","Philippines", "Thailand", "Malaysia")
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
          for (i <- (0 to data.size-2)){
            res += List(System.currentTimeMillis, txid, data(i).id, 1)
            acc += data(i).bankbalance*(1/(data.size-1))
          }
          
          val to =data(data.size-1)
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
    
    val distMap = List(tp1,tp2,tp3,tp4,tp5,tp6,tp7)
    
    for (i <- (1 to 199)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
  }
  
  def runMWorkload(n: Int, ddelay:Long, mdist:List[Boolean], countryLabels:List[String], epool:ExecutorService) {
//    println(n)
//    println(dist.toList)
    val maxm = mdist.map { x => {if (x) 1 else 0} }.reduce(_ + _)
    
    val dist = List(0,0,0,0, 1,1,1,1,1, 2,2, 3,3, 4, 5, 6,6)
//    val pw = new PrintWriter(new File("wl.trace"))
    
    val mtrans = ListBuffer[Long]()
    
    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia","Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")
    
    val distMap = List(tp1,tp2,tp3,tp4,tp5,tp6,tp7)
    val nn = n/10
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          // flip a biased coin to determin if the transaction is malicious
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))  
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }          
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
        
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10,countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10,countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn+1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2,countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn+1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2,countryLabels).foreach { x =>
        {
          if (mtrans.size < maxm && mdist(Random.nextInt(mdist.size))){
            mtrans += x(1)
            val loge = (x ++ List(1L))
            AIMSLogger.logMTxEntry(loge)
            epool.execute(new IDSAlert(loge(1),ddelay))
          }
        }
      }
    }
    
    println(s"Total number of transaction is $n, and malicious ${mtrans.size}")
    
  }
  
  def runNWorkload(n: Int, countryLabels:List[String]) {
    
    val dist = List(0,0,0,0, 1,1,1,1,1, 2,2, 3,3, 4, 5, 6,6)
//    val pw = new PrintWriter(new File("wl.trace"))
    
    // Transaction Profile 1: United States Money Transfer
    val tp1 = List("United States", "Mexico", "Canada")
    val tp2 = List("Germany", "France", "Turkey", "United Kingdom", "Russian Federation", "Italy", "Spain")
    val tp3 = List("Bangladesh", "Pakistan", "China", "India", "South Korea", "Japan", "Vietnam", "Saudi Arabia", "Australia")
    val tp4 = List("Indonesia","Philippines", "Thailand", "Malaysia")
    val tp5 = List("Argentina", "Colombia", "Brazil", "Venezuela")
    val tp6 = List("Kenya", "Nigeria", "Ethiopia", "Congo", "South Africa", "Egypt")
    val tp7 = List("Egypt", "Algeria", "Sudan", "Morocco", "Saudi Arabia")
    
    val distMap = List(tp1,tp2,tp3,tp4,tp5,tp6,tp7)
    val nn = n/10
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassATransaction(countryLabels.indexOf(country1),countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10,countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val country1 = tp(Random.nextInt(tp.size))
      runClassCTransaction(List(country1), 10,countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn+1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2,countryLabels)
    }
    
    for (i <- (1 to nn)){
      val tp = distMap(dist(Random.nextInt(dist.size)))
      val rn = Random.nextInt(tp.size)
      val rn2 = (rn+1) % tp.size
      val country1 = tp(rn)
      val country2 = tp(rn2)
      runClassBTransaction(country1, country2,countryLabels)
    }
    
  }

  // runs a workload according the given distribution
  def runWorkloadDist(dist: List[Int], n: Int) {

  }

  // class A: run a transfer between two random accounts within a given country

  def runClassATransaction(in_ci:Int, countryLabels:List[String]) : List[List[Long]]= {
    val res = ListBuffer[List[Long]]()
    
    var stime = 0L
    var ctime = 0L
    var txid = 0L
    
    var ci =in_ci

    stime = System.currentTimeMillis()
    DB localTx { implicit session =>
      {
        
        
        txid = sql"select txid_current()".list.result(x => {
          x.long(1)
        }, session)(0)
        
        var from = List[Randomdata]() 
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

        sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
        res += List(System.currentTimeMillis(), txid, did, UPDATE_OP, ci)        
      }
    }
    ctime = System.currentTimeMillis()
    AIMSLogger.logResponseTime(txid, stime, ctime)
    res.foreach { AIMSLogger.logTxAccessEntry(_) }
    return res.toList
  }

  // class B: run a transfer between two random accounts from two different countries. 

  def runClassBTransaction(_scountry: String, _dcountry: String, countryLabels:List[String]): List[List[Long]] = {
    val res = ListBuffer[List[Long]]()
    var stime = 0L
    var ctime = 0L
    var txid = 0L
    
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

        res += List(System.currentTimeMillis, txid, from(0).id, READ_OP)

        val to = withSQL {
          select
            .from(Randomdata as r)
            .where.eq(r.country, _dcountry)
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
        res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP)

        sql"update Randomdata set bankbalance = ${dnbal} where id = ${did}".update.apply()
        res += List(System.currentTimeMillis, txid, to(0).id, UPDATE_OP)
      }
    }
    
    ctime = System.currentTimeMillis()

    return res.toList
  }

  // class C: run a transfer between one random account to a random number between 2 and N where N > 5 
  // source and destination can be from different country or same country or mixed

  def runClassCTransaction(_countryList: List[String], _N: Int, countryLabels:List[String]): List[List[Long]] = {

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
            
            val damnt = amnt/rN
            
            val sid = from(0).id
            
            sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
            res += List(System.currentTimeMillis, txid, from(0).id, UPDATE_OP)
            
            to.foreach { tox => {
              val did = tox.id
              sql"update Randomdata set bankbalance = ${damnt} where id = ${did}".update.apply()
              res += List(System.currentTimeMillis, txid, did, UPDATE_OP)              
            } }
                        
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

  def runClassDTransaction(_countryList: List[String], _N: Int, countryLabels:List[String]): List[List[Long]] = {
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

            from.foreach { x => {
              res += List(System.currentTimeMillis, txid, x.id, READ_OP)
            } }

            val to = withSQL {
              select
                .from(Randomdata as r)
                .where.eq(r.country, _country)
                .append(sqls"order by RANDOM()")
                .limit(1)
            }.map(Randomdata(r)).list.apply()

            val amnt = from.map { x => {
              val damnt =  (x.bankbalance * 0.1) 
              var snbal = x.bankbalance - damnt
              if (snbal <= 0) snbal = Random.nextFloat() * reinitbal
              val sid = x.id
              sql"update Randomdata set bankbalance = ${snbal} where id = ${sid}".update.apply()
              res += List(System.currentTimeMillis, txid, sid, UPDATE_OP)
              damnt
              } }.reduce(_+_)
              
            
            to.foreach { tox => {
              val did = tox.id
              sql"update Randomdata set bankbalance = ${amnt} where id = ${did}".update.apply()
              res += List(System.currentTimeMillis, txid, did, UPDATE_OP)              
            } }
            
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
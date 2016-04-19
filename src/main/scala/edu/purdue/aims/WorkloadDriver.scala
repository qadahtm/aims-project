package edu.purdue.aims

/**
 * @author qadahtm
 */

import scalikejdbc._
import org.fluttercode.datafactory.impl.DataFactory
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.File
import java.util.concurrent.Executors
import javax.transaction.xa.Xid;
import java.nio.ByteBuffer

class WorkloadDriver {

}

object DemoWorkloadDriver extends App {
  //  val url = "jdbc:postgresql://localhost:5432/tpcc"
  //  val props = new java.util.Properties()
  //  props.setProperty("user","postgres");
  //  props.setProperty("password","postgres");
  //  props.setProperty("ssl","true");
  //  
  //  
  //  val conn = DriverManager.getConnection(url, props);

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/tpcc", "postgres", "postgres")

  val epool = Executors.newFixedThreadPool(5);

  implicit val session = AutoSession

  GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(
    enabled = false,
    singleLineMode = true,
    logLevel = 'DEBUG)

  val (countryLabels, countryDist) = loadCountries();
  println("Country data loaded. Total coutnries: " + countryLabels.size)
  //  println(countryDist.size)

  if (args.size == 0) {
    printUsage()
    sys.exit(0)
  }

  val mode = args(0)

  mode match {
    case "init" => {
      sql"""
  create table Randomdata (
    id serial not null primary key,
    fname varchar(64) not null,
    lname varchar(64) not null,
    bname varchar(64) not null,
    dob date not null,
    ncars SMALLINT not null,
    bankbalance FLOAT not null,
    addressline varchar(256) not null,
    city varchar(64) not null,
    zipcode varchar(10) not null,
    country varchar(64) not null
  )
  """.execute.apply()

      println("Created Randomdata table")
    }
    case "load" => {
      println("Loading initial data")
      val n = args(1).toInt

      // insert n random tuples
      val df = new DataFactory();
      val mindt = new DateTime("1990-01-01")
      val maxdt = new DateTime("2010-12-31")

      val sdf = new SimpleDateFormat("dd/M/yyyy");
      val r = scala.util.Random
      
      var ci = 0
      for (i <- (1 to n)) {
        val fname = df.getFirstName()
        val lname = df.getLastName

        val dob = df.getDateBetween(mindt.toDate(), maxdt.toDate())

        val ncar = r.nextInt(10)
        val bbalance = r.nextFloat() * r.nextInt(100000)
        val business = df.getBusinessName()
        val address = df.getAddress()
        val city = df.getCity()
        val zipcode = df.getNumberText(5)
        
        ci = ci % countryLabels.size
        val country = countryLabels(ci)

        //         println(s"$fname $lname")
        //         println("dob : "+sdf.format(dob))
        //         println(business + " located at " + address)
        //         println(city + " , "+zipcode)
        //         println(country)

        sql"insert into randomdata (fname, lname, bname, dob, ncars, bankbalance, addressline, city, zipcode, country) values (${fname}, ${lname}, ${business}, ${dob}, ${ncar}, ${bbalance}, ${address}, ${city}, ${zipcode}, ${country})".update.apply()
        ci = ci + 1
      }
    }

    case "runc" => {
      println("Running complex workload")
      val n = args(1).toInt

      TxnUtils.runNWorkload(n, countryLabels)

    }

    case "runm-abs" => {
      val mtxCount = args(1).toInt

    }

    case "runconv" => {
      val n1 = args(1).toInt
      val n2 = args(2).toInt
      
      TxnUtils.runConfWorkloadTry2(countryLabels, n1, n2)
//      TxnUtils.runConvWorkload(countryLabels)
    }

    case "runm" => {
      val dist = Array.ofDim[Boolean](100)
      val n = args(1).toInt
      val ddelay = args(3).toLong
      assert(n % 100 == 0)
      val pmf = args(2).toDouble
      assert(pmf > 0.0 && pmf < 1.0)
      val pm = (pmf * 100).toInt
      //      println(pm)

      for (i <- (0 to pm - 1)) {
        dist(i) = true
      }

      TxnUtils.runMWorkload(n, ddelay, dist.toList, countryLabels, epool)

    }

    case "run" => {
      println("Running Simple Workload")

      val n = args(1).toInt
      val nn = (1 to n / 10)
      //      println(nn)

      //      val pw = new PrintWriter(new File("wl.trace"))

      val alpha = "ABCDEDGHIJKLMNOPRSTUVWZ"
      val rand = scala.util.Random

      // 10 transaction classes

      // 1. updates by last name starting with A,B,C
      // Randomly chooses two letters from ABC
      // add 10% of the balance from the first to the second balance

      val tc1_alpha1 = alpha(rand.nextInt(3)) // index of c
      val tc1_alpha2 = alpha(rand.nextInt(3)) // index of c
      //      println(tc1_alpha1)
      //      println(tc1_alpha2)

      TxnUtils.runTxnInstance(tc1_alpha1.toString(), tc1_alpha2.toString(), n)

      // 2. updates by last name starting with D,E,F

      val tc2_alpha1 = alpha(rand.nextInt(3) + 3) // index of F
      val tc2_alpha2 = alpha(rand.nextInt(3) + 3) // index of F

      TxnUtils.runTxnInstance(tc2_alpha1.toString(), tc2_alpha2.toString(), n)

      // 3. updates by last name starting with G,H,I, J

      val tc3_alpha1 = alpha(rand.nextInt(4) + 6) // index of F
      val tc3_alpha2 = alpha(rand.nextInt(4) + 6) // index of F

      TxnUtils.runTxnInstance(tc3_alpha1.toString(), tc3_alpha2.toString(), n)

      // 4. updates by last name starting with K,L,M, N, O

      val tc4_alpha1 = alpha(rand.nextInt(5) + 10)
      val tc4_alpha2 = alpha(rand.nextInt(5) + 10)

      TxnUtils.runTxnInstance(tc4_alpha1.toString(), tc4_alpha2.toString(), n)

      // 5. updates by last name starting with P,R,S,T,U,V,W,Z

      val tc5_alpha1 = alpha(rand.nextInt(11) + 12)
      val tc5_alpha2 = alpha(rand.nextInt(11) + 12)

      TxnUtils.runTxnInstance(tc5_alpha1.toString(), tc5_alpha2.toString(), n)

      // 6. updates by first name starting with A,B,C

      val tc6_alpha1 = alpha(rand.nextInt(3)) // index of c
      val tc6_alpha2 = alpha(rand.nextInt(3)) // index of c

      TxnUtils.runTxnInstance(tc6_alpha1.toString(), tc6_alpha2.toString(), n)

      // 7. updates by first name starting with D,E,F

      val tc7_alpha1 = alpha(rand.nextInt(3) + 3) // index of F
      val tc7_alpha2 = alpha(rand.nextInt(3) + 3) // index of F

      TxnUtils.runTxnInstance(tc7_alpha1.toString(), tc7_alpha2.toString(), n)

      // 8. updates by first name starting with G,H,I, J,

      val tc8_alpha1 = alpha(rand.nextInt(4) + 6)
      val tc8_alpha2 = alpha(rand.nextInt(4) + 6)

      TxnUtils.runTxnInstance(tc8_alpha1.toString(), tc8_alpha2.toString(), n)

      // 9. updates by first name starting with K,L,M, N, O

      val tc9_alpha1 = alpha(rand.nextInt(5) + 10)
      val tc9_alpha2 = alpha(rand.nextInt(5) + 10)

      TxnUtils.runTxnInstance(tc9_alpha1.toString(), tc9_alpha2.toString(), n)

      // 10. updates by first name starting with P,R,S,T,U,V,W,Z

      val tc10_alpha1 = alpha(rand.nextInt(11) + 12)

      val tc10_alpha2 = alpha(rand.nextInt(11) + 12)

      TxnUtils.runTxnInstance(tc10_alpha1.toString(), tc10_alpha2.toString(), n)

    }

    case "gend" => {

      val in_datacsv = args(1)
      val skipHeader = args(2).toBoolean
      AimsUtils.generateAMPLEDataFiles(in_datacsv, skipHeader)
      println("Done with data file generation!")

    }

    case _ => {
      printUsage()
    }
  }

  def loadCountries(): (List[String], List[Int]) = {
    val lines = scala.io.Source.fromFile("countries_data.csv").getLines()
    lines.next() // skipp header

    val resLabels = ListBuffer[String]()
    val resDist = ListBuffer[Int]()

    var i = 0
    for (line <- lines) {
      val sline = line.split(",")
      resLabels += sline(0)
      val n = sline(1).toInt

      for (j <- (1 to n)) {
        resDist += i
      }

      i = i + 1
    }

    return (resLabels.toList, resDist.toList)

  }

  //   create table Randomdata (
  //    id serial not null primary key,
  //    fname varchar(64) not null,
  //    lname varchar(64) not null,
  //    bname varchar(64) not null,
  //    dob date not null,
  //    ncars SMALLINT not null,
  //    bankbalance FLOAT not null,
  //    addressline varchar(256) not null,
  //    city varchar(64) not null,
  //    zipcode varchar(10) not null
  //  )

  def printUsage() = {
    println("Usage: DemoWorkloadDriver [init|load n|run m] , n = number of data objects to insert, m number of transaction instances to run, m must be a multiple of 10")
  }

  AIMSLogger.closeLoggers()
  epool.shutdown()

}

case class Randomdata(id: Long, fname: String, lname: String, bname: String,
                      dob: java.util.Date, ncars: Int, bankbalance: Float, addressline: String, city: String, zipcode: String, country: String)

object Randomdata extends SQLSyntaxSupport[Randomdata] {
  override val tableName = "Randomdata"

  def apply(o: SyntaxProvider[Randomdata])(rs: WrappedResultSet): Randomdata = apply(o.resultName)(rs)

  //  def apply(o: ResultName[Randomdata])(rs: WrappedResultSet): Randomdata = {
  //    new Randomdata(rs.long(o.id), rs.string(o.fname), rs.string(o.lname), rs.string(o.bname),
  //      rs.date(o.dob), rs.int(o.ncars), rs.float(o.bankbalance),
  //      rs.string(o.addressline), rs.string(o.city), rs.string(o.zipcode), rs.string(o.country))
  //  }

  def apply(o: ResultName[Randomdata])(rs: WrappedResultSet): Randomdata = {
    new Randomdata(rs.long(1), rs.string(2), rs.string(3), rs.string(4),
      rs.date(5), rs.int(6), rs.float(7),
      rs.string(8), rs.string(9), rs.string(10), rs.string(11))
  }
}

class IDSAlert(txid: Long, delay: Long) extends Runnable {
  implicit val session = AutoSession
  def run() {
//    println(s"alerting in $delay ms")
    Thread.sleep(delay)
//    println("alerting now!! "+txid+ " is malicious")
    val ret = sql"select alertMTxn(${txid});".execute().apply()
//    println("done alerting, ret = "+ret)

  }
}
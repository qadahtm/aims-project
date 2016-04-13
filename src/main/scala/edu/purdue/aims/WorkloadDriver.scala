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


class WorkloadDriver {
  
}

object DemoWorkloadDriver extends App{
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
  
  implicit val session = AutoSession
  
  GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(
    enabled = true,
    singleLineMode = true,
    logLevel = 'DEBUG
  )
  
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
    zipcode varchar(10) not null
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
       for (i <- (1 to n)){
         val fname = df.getFirstName()
         val lname = df.getLastName
         
         val dob = df.getDateBetween(mindt.toDate(), maxdt.toDate())
         
         val ncar = r.nextInt(10)
         val bbalance = r.nextFloat()*r.nextInt(100000)
         val business = df.getBusinessName()
         val address = df.getAddress()
         val city = df.getCity()
         val zipcode = df.getNumberText(5)
         
//         println(s"$fname $lname")
//         println("dob : "+sdf.format(dob))
//         println(business + " located at " + address)
//         println(city + " , "+zipcode)
         
         sql"insert into randomdata (fname, lname, bname, dob, ncars, bankbalance, addressline, city, zipcode) values (${fname}, ${lname}, ${business}, ${dob}, ${ncar}, ${bbalance}, ${address}, ${city}, ${zipcode})".update.apply()
       }                
    }
    case "run" => {
      println("Running Workload")
      
      val n = args(1).toInt
      val nn = (1 to n/10)
//      println(nn)
      
      val pw = new PrintWriter(new File("wl.trace"))
      
      val alpha = "ABCDEDGHIJKLMNOPRSTUVWZ"
      val rand = scala.util.Random
    
      // 10 transaction profiles
      
   
      def runTxnInstance(tc_alpha1:String, tc_alpha2:String, n:Int) : List[List[Long]] = {
        val r = Randomdata.syntax("r")
        val nn = (1 to n/10)
        var res = ListBuffer[List[Long]]()
        for (i <- nn) {
          DB localTx { implicit session =>
            {
  
              val txid = sql"select txid_current()".list.result(x => {
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
              val re = List( System.currentTimeMillis, txid, from(0).id, 1)
              val we = List( System.currentTimeMillis, txid, to(0).id, 3)
              res += re
              res += we
            }
          }
        }
        
        return res.toList        
      }
      
      // 1. updates by last name starting with A,B,C
      // Randomly chooses two letters from ABC
      // add 10% of the balance from the first to the second balance
      
      val tc1_alpha1 = alpha(rand.nextInt(3)) // index of c
      val tc1_alpha2 = alpha(rand.nextInt(3)) // index of c
//      println(tc1_alpha1)
//      println(tc1_alpha2)
      
      runTxnInstance(tc1_alpha1.toString(), tc1_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 2. updates by last name starting with D,E,F
      
          
      val tc2_alpha1 = alpha(rand.nextInt(3)+3) // index of F
      val tc2_alpha2 = alpha(rand.nextInt(3)+3) // index of F
      
      runTxnInstance(tc2_alpha1.toString(), tc2_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 3. updates by last name starting with G,H,I, J
      
      val tc3_alpha1 = alpha(rand.nextInt(4)+6) // index of F
      val tc3_alpha2 = alpha(rand.nextInt(4)+6) // index of F
      
      runTxnInstance(tc3_alpha1.toString(), tc3_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 4. updates by last name starting with K,L,M, N, O
      
      val tc4_alpha1 = alpha(rand.nextInt(5)+10) 
      val tc4_alpha2 = alpha(rand.nextInt(5)+10) 
      
      runTxnInstance(tc4_alpha1.toString(), tc4_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      
      // 5. updates by last name starting with P,R,S,T,U,V,W,Z
      
      val tc5_alpha1 = alpha(rand.nextInt(11)+12) 
      val tc5_alpha2 = alpha(rand.nextInt(11)+12) 
      
      runTxnInstance(tc5_alpha1.toString(), tc5_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 6. updates by first name starting with A,B,C
      
      val tc6_alpha1 = alpha(rand.nextInt(3)) // index of c
      val tc6_alpha2 = alpha(rand.nextInt(3)) // index of c
      
      runTxnInstance(tc6_alpha1.toString(), tc6_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 7. updates by first name starting with D,E,F
      
      val tc7_alpha1 = alpha(rand.nextInt(3)+3) // index of F
      val tc7_alpha2 = alpha(rand.nextInt(3)+3) // index of F
      
      runTxnInstance(tc7_alpha1.toString(), tc7_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 8. updates by first name starting with G,H,I, J,
      
      val tc8_alpha1 = alpha(rand.nextInt(4)+6) 
      val tc8_alpha2 = alpha(rand.nextInt(4)+6) 
      
      runTxnInstance(tc8_alpha1.toString(), tc8_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 9. updates by first name starting with K,L,M, N, O
      
      val tc9_alpha1 = alpha(rand.nextInt(5)+10) 
      val tc9_alpha2 = alpha(rand.nextInt(5)+10) 
      
      runTxnInstance(tc9_alpha1.toString(), tc9_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.flush()
      
      // 10. updates by first name starting with P,R,S,T,U,V,W,Z
      
      val tc10_alpha1 = alpha(rand.nextInt(11)+12) 
      
      val tc10_alpha2 = alpha(rand.nextInt(11)+12) 
      
      runTxnInstance(tc10_alpha1.toString(), tc10_alpha2.toString(),n).foreach{x => {
        pw.println(x.mkString(","))
      }}
      
      pw.close()
    }
    
    case "gend" => {
      
      val in_datacsv = args(1)
      val skipHeader = args(2).toBoolean
      AimsUtils.generateDataFiles(in_datacsv, skipHeader)
    }
    
    case _ => {
      printUsage()
    }
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
  case class Randomdata(id: Long, fname: String, lname: String, bname: String,
      dob:java.util.Date, ncars:Int, bankbalance:Float, addressline:String, city:String, zipcode:String)
      
      
  object Randomdata extends SQLSyntaxSupport[Randomdata] {
    override val tableName = "Randomdata"
    
    def apply(o: SyntaxProvider[Randomdata])(rs: WrappedResultSet): Randomdata = apply(o.resultName)(rs)
    
//    def apply(rs: WrappedResultSet) = new Randomdata(
//      rs.long("id"), rs.string("fname"), rs.string("lname"), rs.string("bname"),
//      rs.date("dob"), rs.int("ncars"), rs.float("bankbalance"), 
//      rs.string("addressline"), rs.string("city"), rs.string("zipcode"))
    
    def apply(o: ResultName[Randomdata])(rs: WrappedResultSet): Randomdata = {
      new Randomdata(rs.long(o.id), rs.string(o.fname), rs.string(o.lname), rs.string(o.bname),
          rs.date(o.dob), rs.int(o.ncars), rs.float(o.bankbalance), 
          rs.string(o.addressline), rs.string(o.city), rs.string(o.zipcode))
    }
  }
  
  def printUsage() = {
    println("Usage: DemoWorkloadDriver [init|load n|run m] , n = number of data objects to insert, m number of transaction instances to run, m must be a multple of 10")
  }

}
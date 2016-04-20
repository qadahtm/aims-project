/**
 *
 * Copyright 2016 - Thamir Qadah
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package edu.purdue.aims

import org.chocosolver.solver.Solver
import org.chocosolver.solver.constraints.IntConstraintFactory
import org.chocosolver.solver.constraints.set.SetConstraintsFactory
import org.chocosolver.solver.search.strategy.IntStrategyFactory
import org.chocosolver.solver.trace.Chatterbox
import org.chocosolver.solver.variables.VariableFactory
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge.DiEdge
import org.chocosolver.solver.variables.IntVar
import org.chocosolver.solver.variables.SetVar
import org.chocosolver.solver.variables.RealVar
import org.chocosolver.solver.constraints.real.RealConstraint
import org.chocosolver.solver.variables.BoolVar
import scala.collection.mutable.ListBuffer
import org.chocosolver.solver.constraints.extension.Tuples
import org.chocosolver.solver.ResolutionPolicy
import scala.io.Source

import java.io._

object IBDP extends App {
      play()
//  println("IB Demarcation Problem Solver")
//    val in_datacsv = args(0)
//    val in_k = args(1).toInt
//    val in_eps = args(2).toInt
//    val in_qmax = args(3).toInt
  //  chocoExample()

  
//  IBDPSolve(in_datacsv,in_k, in_eps, in_qmax)
  
  
//  IBDP_BinPack()
    
  
  
  def play() = {
    val lst1 = List(1,2,3)
    val lst2 = List(5,6,7,8)
    
    var i = 0
    var j = 0
    while (i < lst1.size){
      
      if (j == lst2.size-1){
        for (kk <- (i to lst1.size-1)){
          println(lst1(kk)+" -> "+lst2(j))           
        }
        
        i = lst1.size // break loop
      }
      else if (i == lst1.size-1){
        for (kk <- (j to lst2.size-1)){
          println(lst1(i)+" -> "+lst2(kk))           
        }
      }
      else
        println(lst1(i)+" -> "+lst2(j))
      j = j + 1
      i = i +1
    }
    
  }
  
  
  
  
  def IBDPSolve(fname:String, in_k:Int, in_eps:Int, in_qmax:Int) = {
    // IBDP Parameters
//    val K = 4 // number of the desired partitions
//    val eps = 5 // maximum difference among partitions (pairwise)
//    val Qmax = 10 // maximum number of boundary objects allowed.
    
    val K = in_k // number of the desired partitions
    val eps = in_eps // maximum difference among partitions (pairwise)
    val Qmax = in_qmax // maximum number of boundary objects allowed.
    
    // Problem Setup
    println("Parsing Workload fom File")
    val fromFile = AimsUtils.readWTFromLogTable(fname, true)
    println("Workload Parsing is done")
    val dataObjectsLabels = fromFile._1
    
//    val dataObjectsLabels = List[String]("a","b","c","d", "e", "f", "g","h","i","j","k","n")    
//    val dataObjectsLabels = List[String]("a","b","c","d")    
    val N = dataObjectsLabels.size
    val doidxs = dataObjectsLabels.zipWithIndex.map((_._2))
    
    // Workload Parameters
    val WT = fromFile._4
//     val WT = sampleWorkload()
//     val WT = tinyWorload()
     val W = WT.size
    println(s"Setting Up Otimization Problem with $N data objects and $W transaction instances")
    // Problem Paramters  
    val ibps = (0 to (K-1)).toList
    val pcombs = combs(ibps, 2).toList
    
    
    // Assignment variables 
    val X = for { o_i <- doidxs
                  p_j <- ibps} 
              yield {(o_i,p_j)}
        
    // 1. Create a Solver
    val ibdp_solver = new Solver("IBDP");
    // 2. Create variables through the variable factory
    
    val vars = (for (x_ij <- X) yield (x_ij, VariableFactory.enumerated(x_ij.toString(), 0, 1, ibdp_solver))).toList
    
    // Adjacency Matrix variables 
    val evarm = Array.ofDim[IntVar](N,N)
    
    // Transaction instance Matrix variables.
    // For each transaction instance, we have a vector, 
    // indicating if an object is accessed by that transaction instance
    
    WT.zipWithIndex.foreach { case (txni,j) => {
      fill1_evarm(txni, evarm, ibdp_solver)
    } }  
    fill0_evarm(evarm, ibdp_solver)
    
    val txn_list = ListBuffer[((Int,Int), IntVar)]()
    for (i <- (0 to N-1)){
      for (j <- (0 to WT.size-1)){
        val tup = ((i,j), get_tvarm_entry(i,j,WT(j),ibdp_solver))
        txn_list += tup
      }
    }
    
//3. Create and post constraints by using constraint factories
    
//All data objects are assigned
    
    ibdp_solver.post(IntConstraintFactory.sum(vars.map(_._2).toArray, ">=",  VariableFactory.fixed(N, ibdp_solver)))
    for (i <- (0 to N-1)){
      val vars_for_oi = vars.filter{case ((o_i,p_j), v) => o_i == i}.map(_._2)
//      println(s"this should be $K = "+vars_for_oi.size)
      
      ibdp_solver.post(IntConstraintFactory.sum(vars_for_oi.toArray, ">=", VariableFactory.fixed(1, ibdp_solver)))
      
      // the following constraint will not allow any boundary objects
//      ibdp_solver.post(IntConstraintFactory.among(VariableFactory.fixed(1,ibdp_solver), vars_for_oi.toArray,Array(1)))
    }
    
      
//Size-balance constraint within eps
    
    val psums = ibps.map { x => {
      VariableFactory.bounded("sum_p_"+x, 0, N, ibdp_solver)      
    } }
    
    val bos = doidxs.map { x => {
      VariableFactory.enumerated("bo_"+x, 0, 1, ibdp_solver)
    } } 
    
    val bocs = doidxs.map { x => {
      VariableFactory.bounded("boc_"+x, 0, K, ibdp_solver)
    } } 
    
   // constraining sizes of IBs
    psums.zipWithIndex.foreach { case (x,j) => {
      // Add constraints 
      val objects_in_px = vars.filter{case ((o_i,p_j), v) => p_j == j}.map(_._2)
      ibdp_solver.post(IntConstraintFactory.sum(objects_in_px.toArray, x))
    } }

//     posting constraints 
    pcombs.map { ppair => {
      val p1sum = psums(ppair(0))
      val p2sum = psums(ppair(1))
      
      ibdp_solver.post(IntConstraintFactory.distance(p1sum, p2sum, "<", eps))
    } }

    
//    // number of boundary nodes per partition
    bocs.zipWithIndex.foreach { case (x,i) => {
      val ps_assigned = vars.filter{case ((o_i,p_j), v) => o_i == i}.map(_._2)
      ibdp_solver.post(IntConstraintFactory.sum(ps_assigned.toArray, x))
    } }
    
//     Constraining maximum number of partitions that objects are assigned to 
//     For all objects, they cannot be assigned to more than Qmax partitions
    bocs.foreach { boc => {
      ibdp_solver.post(IntConstraintFactory.arithm(boc, "<=", Qmax))
    } }
    
    
    // No dependencies allowed among boundary objects
    
    // possible mapping from boc_i to bo_i
    val bo_tuples = new Tuples(true);
    for (_k <- (0 to K)){
      if (_k == 0 || _k == 1){
        bo_tuples.add(_k, 0)
      }
      else{
        bo_tuples.add(_k, 1)
      }
    }
    
    var alg = "AC2001"
    // this will ensure that boi == 1 if there is at least to x_ijs == 1, sum 
    bos.zipWithIndex.foreach { case (bo,i) => {
      val boc = bocs(i)
        ibdp_solver.post(IntConstraintFactory.table(bocs(i), bo, bo_tuples, alg))
    } }

    val borels = ListBuffer[(IntVar,IntVar,IntVar,IntVar)]()
    
    for (c <- combs(bos.zipWithIndex, 2)){
      val (o_i, i) = c(0)
      val (o_j,j) = c(1)
      val tmp_times = VariableFactory.enumerated("bo_times_"+i+"_"+j, 0, 1, ibdp_solver)
      val borel = (o_i,o_j, tmp_times,evarm(i)(j))
      borels += borel
      ibdp_solver.post(IntConstraintFactory.times(o_i, o_j, tmp_times))
      ibdp_solver.post(IntConstraintFactory.times(tmp_times, evarm(i)(j), VariableFactory.fixed(0, ibdp_solver)))
      
    }
    
    // Each transaction instance is contained within an IB
    // Computing sums for transactions
    // for each transaction evaulate the containment in each partition
    // exactly one p_j that contains t_i
    
    val contain_list = ListBuffer[((Int,Int),IntVar)]()
    for (k <- (0 to W-1)){
      for (j <- (0 to K-1)){
        val c = ((k,j), VariableFactory.enumerated("contained", 0,N, ibdp_solver))
        contain_list += c
      }      
    }
    
    for (k <- (0 to W-1)){
      val t_k_fp = txn_list.filter(_._1._2 == k).map(_._2.getValue).toList
      val tk_sum = t_k_fp.foldLeft(0)(_ + _)
      val clist = contain_list.filter(x => x._1._1 == k).toList
      
      for (j <- (0 to K-1)){  
        val fclist = clist.filter(x => x._1._2 == j).map(_._2).toList
        val dotp_k_j = fclist(0)
        val vars_p_j = vars.filter(_._1._2 == j).map(_._2).toArray
        // ensures that dotproduct is computed proparly
        ibdp_solver.post(IntConstraintFactory.scalar(vars_p_j,t_k_fp.toArray, dotp_k_j))
      }
      // ensures that there is at least one p_j that contains t_k
      ibdp_solver.post(IntConstraintFactory.among(VariableFactory.fixed(1,ibdp_solver), clist.map(_._2).toArray.toArray,Array(tk_sum)))
    }
    
    // 3.b. Define the optimization objective function 
    val node_weights = AimsUtils.compute_weight_sets(dataObjectsLabels, WT)
    val obj_var = VariableFactory.bounded("cost_func1", 0, 999, ibdp_solver)
    val nwArr = node_weights.map(_.size).toArray
    ibdp_solver.post(IntConstraintFactory.scalar(bos.toArray, nwArr, obj_var))
    
    
//    // 4. Define the search strategy
     //TBD
    
    
      
//    // 5. Launch the resolution process
    println("Starting the solver")
    ibdp_solver.findOptimalSolution(ResolutionPolicy.MINIMIZE, obj_var)
    
//    if (ibdp_solver.findSolution()){
//      do {
//        
//      println("Found a solution")
      
//      println("Borels Count = "+borels.size)
      
      vars.foreach{ case ((o_i, p_j),v) => {
//        println(s"x($o_i)($p_j) =  "+v.getValue())
        if (v.getValue() > 0) {
           println("object "+dataObjectsLabels(o_i)+" is assigned to partition "+p_j)
        }
        else {
          println("object "+dataObjectsLabels(o_i)+" is NOT assigned to partition "+p_j)
        }
       
      }}    
      
      psums.zipWithIndex.foreach { case (s,i) => {
        println("number of objects assigned to p("+i+") is "+s.getValue())
      }}
      
      bocs.zipWithIndex.foreach { case (x,i) => {
        println("number of partitions assigned to object "+dataObjectsLabels(i)+" is "+x.getValue())
      } }
      
      bos.zipWithIndex.foreach { case (x,i) => {
        if (x.getValue() == 1){
          println("object "+dataObjectsLabels(i)+" is a boundary object")
        }
        
      } }
      println("bo list:"+bos.map(_.getValue))
      println("weight list: "+nwArr.toList)
      var objfunc_val=0
      for (i<- (0 to N-1)){
        objfunc_val += bos(i).getValue*nwArr(i)
      }
      println("objective func = "+obj_var.getValue)
      println("objective func (CSP-Solver) = "+obj_var.getValue)
      
      
      for (j <- (0 to K-1)){
        for (k <- (0 to W-1)){
          val fclist = contain_list.filter(x => x._1._1 == k && x._1._2 == j).map(_._2).toList
          val vars_p_j = vars.filter(_._1._2 == j).map(_._2).toArray
          val t_k_fp = txn_list.filter(_._1._2 == k).map(_._2.getValue).toList
          val tk_sum = t_k_fp.foldLeft(0)(_ + _)
          
          fclist.foreach { x => {
//            println("csp_dot_product = "+x.getValue)
            if (x.getValue == tk_sum){
              println(s"t_$k is contained in p_$j")  
            }
            else {
              println(s"t_$k is NOT contained in p_$j")
            }
            
          } }
          
//          println(s"p_$j : "+vars_p_j.map { _.getValue}.toList)
//          println(s"t_$k fp : "+t_k_fp)
          var dotp_k_j = 0
          for (ii <- (0 to N-1)){
              dotp_k_j += vars_p_j(ii).getValue*t_k_fp(ii)   
          }
//          println("dot_product = "+dotp_k_j+", t_k_sum = "+tk_sum)
//          if (dotp_k_j == tk_sum){
//            println("========================  A solution above =========================")
//          }
        }
      }
        
//      } while (ibdp_solver.nextSolution())
//    }
    Chatterbox.printStatistics(ibdp_solver);
  }
  
   // create a list of combinations of elements of a given list
  def combs[A](l: List[A], n: Int): Iterator[List[A]] = n match {
    case _ if n < 0 || l.lengthCompare(n) < 0 => Iterator.empty
    case 0 => Iterator(List.empty)
    case n => l.tails.flatMap({
      case Nil => Nil
      case x :: xs => combs(xs, n - 1).map(x :: _)
    })
  }
  
  def getTxniFP(k:Int,txn_list:List[((Int,Int),IntVar)]): List[((Int,Int),IntVar)] = {
    txn_list.filter{case ((i,j),v) => j == k}.toList
  }
  
  def fill_tvarm(txni:TransactionInstance, j:Int , tvarm:Array[Array[IntVar]], _solver:Solver): Unit = {
    val rset = txni.rs.map { x => x.oid }.toSet
    val wset = txni.ws.map { x => x.oid }.toSet
    
    for (i <- (0 to tvarm(j).size-1)) {
      if (rset.contains(i) || wset.contains(i)) {
        tvarm(i)(j) = VariableFactory.fixed(1, _solver)
      }
      else {
        tvarm(i)(j) = VariableFactory.fixed(0, _solver)
      } 
    }
  }
  
  def get_tvarm_entry(i:Int, j:Int , txni:TransactionInstance, _solver:Solver): IntVar = {
    val rset = txni.rs.map { x => x.oid }.toSet
    val wset = txni.ws.map { x => x.oid }.toSet
    if (rset.contains(i) || wset.contains(i)) {
       return VariableFactory.fixed(1, _solver)
    }
    else {
      return VariableFactory.fixed(0, _solver)
    } 
  }
  
  def fill0_evarm(evarm:Array[Array[IntVar]] , _solver:Solver) = {
    // fill in zeros 
    for (i <- (0 to evarm.size-1)){
      for (j <- (0 to evarm(i).size-1)){
        if (evarm(i)(j) == null){
          evarm(i)(j) = VariableFactory.fixed(0, _solver)
        }
      }
    }
  }
  
  def fill1_evarm(txni:TransactionInstance, evarm:Array[Array[IntVar]] , _solver:Solver) = {
    val rset = txni.rs.map { x => x.oid }.toSet
    val wset = txni.ws.map { x => x.oid }.toSet
    val rdiff = rset.diff(wset)
    val wdiff = wset.diff(rset)
    
    for (rse <- rdiff){
      for (wse <- wdiff){
        evarm(rse)(wse) = VariableFactory.fixed(1, _solver)
      }
    }    
  }
  
  def compute_edges_txni(txni:TransactionInstance): List[DiEdge[Int]] = {
    val rset = txni.rs.map { x => x.oid }.toSet
    val wset = txni.ws.map { x => x.oid }.toSet
    val rdiff = rset.diff(wset)
    val wdiff = wset.diff(rset)
    
    var res = collection.mutable.ListBuffer[DiEdge[Int]]()
    for (rse <- rdiff){
      for (wse <- wdiff){
        val e = rse~>wse
        res += e
      }
    }
    return res.toList
  }
  
  

  val dataObjectsLabels = List[String]("a","b","c","d", "e", "f", "g","h","i","j","k","n")
  
  
//  a = 0
//  b = 1
//  c = 2
//  d = 3
//  e = 4
//  f = 5
//  g = 6
//  h = 7
//  i = 8
//  j = 9
//  k = 10
//  n = 11
  
  def tinyWorload() = List(
      TransactionInstance(0,0,
          List(DataObject(0)),
          List(DataObject(1))),
      TransactionInstance(1,0,
          List(DataObject(0)),
          List(DataObject(2))),          
      TransactionInstance(2,0,
          List(DataObject(2)),
          List(DataObject(3)))
      )
      
      
  def createDataFiles() = {
    
  }
  
  def sampleWorkload() = List(
      TransactionInstance(0,0,
          List(DataObject(4), DataObject(7), DataObject(11)),
          List(DataObject(8))),
      TransactionInstance(1,0,
          List(DataObject(7)),
          List(DataObject(0))),          
      TransactionInstance(1,1,
          List(DataObject(8)),
          List(DataObject(10))),
      TransactionInstance(2,0,
          List(DataObject(2), DataObject(4), DataObject(8)),
          List(DataObject(5))),
      TransactionInstance(3,0,
          List(DataObject(0)),
          List(DataObject(4))),
      TransactionInstance(4,0,
          List(DataObject(1)),
          List(DataObject(0))),
      TransactionInstance(5,0,
          List(DataObject(9)),
          List(DataObject(6))),
      TransactionInstance(6,0,
          List(DataObject(0), DataObject(3)),
          List(DataObject(3))),
      TransactionInstance(7,0,
          List(DataObject(1), DataObject(10)),
          List()),
      TransactionInstance(8,0,
          List(DataObject(4)),
          List(DataObject(2))),
      TransactionInstance(9,0,
          List(DataObject(3)),
          List(DataObject(7))),
      TransactionInstance(9,1,
          List(DataObject(6)),
          List())
      )
      
}
case class DataObject(val oid:Int)
case class TransactionInstance(val cid:Int, val ts:Int, val rs:List[DataObject], val ws:List[DataObject])
case class TransactionFootprint(val cid:Int, val rs:List[DataObject], val ws:List[DataObject])


object AimsUtils {
  
  def genAMPLEDatFile(in_datacsv:String, skipHeader:Boolean, k:Int, q:Int){
    println("Parsing workload trace file")
    
    val parsed =  readWTFromLogTable(in_datacsv, skipHeader)
    val dolabels = parsed._1
    val txnlabels = parsed._2
    val txnClasslabels = parsed._3
    val WTres = parsed._4
    
    println("number of classes ="+txnClasslabels.size)
//    txnClasslabels.foreach { println }
    println("number of tx instance ="+WTres.size)
    
//    for (ti <- WTres){
//      println(s"${ti.cid}, ${ti.ts}, rs = ${ti.rs}, ws = ${ti.ws}")
//    }
    
    val WTFP = WTres.map { x => {
      TransactionFootprint(x.cid, x.rs,x.ws)
    } }.groupBy { x => {
      x.cid
    } }.mapValues { x => {
      var res = x(0)
      for (i <- (1 to x.size-1)){
        res = TransactionFootprint(res.cid, res.rs.union(x(i).rs), res.ws.union(x(i).ws))
      }
      res
    } }.map { _._2 }.toList
    
    val N = dolabels.size
    
    // creating file for transaction instances
//    println("creating transaction fps file block")
    
//    val fpa = Array.ofDim[Int](dolabels.size)
//    val pw_t = new PrintWriter(new File("t.dat" ))
//    pw_t.print("txncId")
//    for (o_i <- dolabels){
//      pw_t.print(s",$o_i")  
//    }
//    pw_t.println()
//    for (txni <- WTFP) {
//      // create a datafile for T
//      pw_t.print(txnClasslabels(txni.cid))
//      for (i <- (0 to dolabels.size-1)){
//        pw_t.print(",")
////        if (txni.fp.map(_.oid).contains(i)){
//        if (txni.rs.map(_.oid).contains(i) || txni.ws.map(_.oid).contains(i)){
//          pw_t.print("1")  
//          fpa(i) = fpa(i) + 1
//        }
//        else pw_t.print("0")        
//      }
//      
//      pw_t.println()     
//      pw_t.flush()
//    }
//     pw_t.close
//     
//     // asset that all data objects are assigned to some footprint
//    for (o_i <- fpa){
//      assert(o_i > 0)
//    }
     
    // creating files for edges
//     println("creating edges file")
////    println("DDG")
//   val pw_e = new PrintWriter(new File("e.dat" ))
//    for ((o_i,i) <- dolabels.zipWithIndex){
//      if (i >0) pw_e.print(",")
//      pw_e.print(s"$o_i")  
//    }
//    pw_e.println()
//    
    val evarm = Array.ofDim[Int](N,N)
    WTres.zipWithIndex.foreach { case (txni,j) => {
      fill1_evarmInt(txni, evarm)
    } }  
    
//    for (i <- (0 to evarm.size-1)){      
//      for (j <- (0 to evarm(i).size-1)){
////        if (evarm(i)(j) == 1) println(s"edge between o($i) and o($j)")
//        if (j > 0) pw_e.print(",")
//        pw_e.print(evarm(i)(j))
//      }      
//      pw_e.println()
//      pw_e.flush()
//    }
//   
//    pw_e.close
//    
    
    // creating data file for weights
//    println("creating weights file")
//    val pw_w = new PrintWriter(new File("w.dat" ))
//    println("going to compute weights")
    val weights = compute_weights(dolabels, WTres)
//    println("done: weights are computed")
//    
//    var i = 0
//    for(w_i <- weights) {
//      pw_w.println(dolabels(i) +","+w_i )
//      i = i +1
//    }
//    
//    pw_w.close
    
    
    val pw = new PrintWriter(new File("syng.dat"))
    // creating header block
    println("creating AMPLE data file")
    println("creating header")
    val sb = new StringBuilder()
    sb.append("set K := ")
    for (i <- (0 to k-1)) {
      if (i > 0) sb.append(" ")
      sb.append("k"+i)
    }
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
    
    sb.append("set N := ")
    for (i <- (0 to N-1)){
      if (i > 0) sb.append(" ")
      sb.append("n"+i)      
    }
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
    
    val L = WTFP.size
    
    sb.append("set L := ")
    for (i <- (0 to L-1)){
      if (i > 0) sb.append(" ")
      sb.append("l"+i)      
    }
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
    
    sb.append("param n := ")
    sb.append(dolabels.size)
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
    
    sb.append("param k := ")
    sb.append(k)
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
    
    sb.append("param q := ")
    sb.append(q)
    sb.append(";")
    pw.println(sb.toString())
    sb.clear()
     
    pw.println("# DDG (adjacency matrix)")
    pw.println("param E:")
    
    sb.append("     ") // space before 
    for (i <- (0 to N-1)){
      if (i > 0) sb.append(" ")
      sb.append("n"+i)      
    }
    sb.append(" :=")
    pw.println(sb.toString())
    sb.clear()
    
    // print e-values for each row
    for (i <- (0 to N-1)){
      // print label first 
      sb.append("n"+i)
      
      for (j <- (0 to N-1)){
         sb.append(" ")
         sb.append(evarm(i)(j))
      }      
      
      if (i == N-1) sb.append(";")
      pw.println(sb.toString())
      sb.clear()      
    }
    
    pw.println("# Vertices weight vector")
    pw.println("param W:")
    
    sb.append("     ") // space before 
    for (i <- (0 to N-1)){
      if (i > 0) sb.append(" ")
      sb.append("n"+i)
    }
    sb.append(" :=")
    pw.println(sb.toString())
    sb.clear()
    
    sb.append("w1  ")
    for(w_i <- weights) {
      sb.append(" "+w_i)
    }
    sb.append(";")
    
    pw.println(sb.toString())
    sb.clear()
    
    pw.println("# Transaction assignment")
    pw.println("param T:")
    
    sb.append("     ") // space before 
    for (i <- (0 to N-1)){
      if (i > 0) sb.append(" ")
      sb.append("n"+i)
    }
    sb.append(" :=")
    pw.println(sb.toString())
    sb.clear()
    
    for (i <- (0 to L-1)){
      // print label first 
      sb.append("l"+i)

      val lfp = WTFP(i)
      
      for (j <- (0 to N-1)){
         sb.append(" ")
         if (lfp.rs.map(_.oid).contains(j) || lfp.ws.map(_.oid).contains(j)){
           sb.append("1")
         }
         else{
           sb.append("0")
         }
      }   
      
      if (i == L-1) sb.append(";")
      pw.println(sb.toString())
      sb.clear()      
    }
    
    
    
    
    
    pw.close()
  }
  
   def generateAMPLEDataFiles(in_datacsv:String, skipHeader:Boolean) ={
    println("Parsing workload trace file")
//    var parsed:(List[String], List[String], )
    
    val parsed =  readWTFromLogTable(in_datacsv, skipHeader)
    val dolabels = parsed._1
    val txnlabels = parsed._2
    val txnClasslabels = parsed._3
    val WTres = parsed._4
    
    println("number of classes ="+txnClasslabels.size)
//    txnClasslabels.foreach { println }
    println("number of tx instance ="+WTres.size)
    
//    for (ti <- WTres){
//      println(s"${ti.cid}, ${ti.ts}, rs = ${ti.rs}, ws = ${ti.ws}")
//    }
    
    val WTFP = WTres.map { x => {
      TransactionFootprint(x.cid, x.rs,x.ws)
    } }.groupBy { x => {
      x.cid
    } }.mapValues { x => {
      var res = x(0)
      for (i <- (1 to x.size-1)){
        res = TransactionFootprint(res.cid, res.rs.union(x(i).rs), res.ws.union(x(i).ws))
      }
      res
    } }.map { _._2 }
    
    val N = dolabels.size
    
    // creating file for transaction instances
    println("creating transaction fps file")
    
    val fpa = Array.ofDim[Int](dolabels.size)
    val pw_t = new PrintWriter(new File("t.dat" ))
    pw_t.print("txncId")
    for (o_i <- dolabels){
      pw_t.print(s",$o_i")  
    }
    pw_t.println()
    for (txni <- WTFP) {
      // create a datafile for T
      pw_t.print(txnClasslabels(txni.cid))
      for (i <- (0 to dolabels.size-1)){
        pw_t.print(",")
//        if (txni.fp.map(_.oid).contains(i)){
        if (txni.rs.map(_.oid).contains(i) || txni.ws.map(_.oid).contains(i)){
          pw_t.print("1")  
          fpa(i) = fpa(i) + 1
        }
        else pw_t.print("0")        
      }
      
      pw_t.println()     
      pw_t.flush()
    }
     pw_t.close
     
     // asset that all data objects are assigned to some footprint
    for (o_i <- fpa){
      assert(o_i > 0)
    }
     
    // creating files for edges
     println("creating edges file")
//    println("DDG")
   val pw_e = new PrintWriter(new File("e.dat" ))
    for ((o_i,i) <- dolabels.zipWithIndex){
      if (i >0) pw_e.print(",")
      pw_e.print(s"$o_i")  
    }
    pw_e.println()
    
    val evarm = Array.ofDim[Int](N,N)
    WTres.zipWithIndex.foreach { case (txni,j) => {
      fill1_evarmInt(txni, evarm)
    } }  
    
    for (i <- (0 to evarm.size-1)){      
      for (j <- (0 to evarm(i).size-1)){
//        if (evarm(i)(j) == 1) println(s"edge between o($i) and o($j)")
        if (j > 0) pw_e.print(",")
        pw_e.print(evarm(i)(j))
      }      
      pw_e.println()
      pw_e.flush()
    }
   
    pw_e.close
    
    
    // creating data file for weights
    println("creating weights file")
    val pw_w = new PrintWriter(new File("w.dat" ))
    println("going to compute weights")
    val weights = compute_weights(dolabels, WTres)
    println("done: weights are computed")
    
    var i = 0
    for(w_i <- weights) {
      pw_w.println(dolabels(i) +","+w_i )
      i = i +1
    }
    
    pw_w.close
    
    
  }
   
   // compute weights on nodes from transaction workload
  // weights correspond to transaction instances
  // returns a pair (i,w) where i = index of data object, w = weight of data object (not used) 
  // returns a pair (i,w_set) where i = index of data object, w = a set of transaction instance ids 
  
  def compute_weight_sets(D:List[String], WT:List[TransactionInstance]) : List[Set[Int]]={
    val res = ListBuffer[Set[Int]]()
    for (i <- (0 to D.size-1)){
      val wset = collection.mutable.Set[Int]()
      for (j <- (0 to WT.size-1)){
//        println(WT(j))
        if(WT(j).rs.map(_.oid).toSet.contains(i) || 
            WT(j).ws.map(_.oid).toSet.contains(i)){
//          println(s"t_$j in the weight set of o_$i")
          wset += j
        }
      }
      
      res += wset.toSet
    }
    res.toList
  }
  
  
  def compute_weights(D:List[String], WT:List[TransactionInstance]) : List[Int]={
    val res = Array.ofDim[Int](D.size)
    for (j <- (0 to WT.size-1)){
      val dos = WT(j).rs.map(_.oid).toSet.union( WT(j).ws.map(_.oid).toSet)      
      for (o_i <- dos){
        res(o_i) = res(o_i) + 1        
      }        
     }
    res.toList
  }
  
  
  // Example: readWTFromLogTable("log_table_tpcc_small.csv", True)
  
  def readWTFromLogTable(fname:String, skipHeader: Boolean) : (List[String], List[String], List[String], List[TransactionInstance]) = {
//    val fname = "log_table_tpcc_small.csv"    
    val reader = Source.fromFile(fname)
    
    val WT = collection.mutable.Map[Int,TransactionInstance]()
    val txnLabels = ListBuffer[String]()
    val dataObjectsLabels = ListBuffer[String]()
    val txnClassLabels = ListBuffer[String]()
    
    val lines = reader.getLines()
    if (skipHeader) lines.next() // skip the header line
    lines.foreach{ line => {
      
      val sline = line.split(",")
      val txid = sline(1)
      val oid = sline(2)
      val op = sline(3)
      val cidstr = sline(4)
      
      // add labels if do not exist
      if (!txnClassLabels.toSet.contains(cidstr)) txnClassLabels += cidstr
      if (!txnLabels.toSet.contains(txid)) txnLabels += txid
      if (!dataObjectsLabels.toSet.contains(oid)) dataObjectsLabels += oid
      
      val j = txnLabels.indexOf(txid)
      val i = dataObjectsLabels.indexOf(oid)  
      val cid = txnClassLabels.indexOf(cidstr)
      
      WT.get(j) match {
        case Some(txni) => {
          // we have seen this transaction instance before, update its rs and ws
          if (op.toInt == 1){
            if (!txni.rs.map(_.oid).toSet.contains(i)){
//              println(s"tx $j has read object $i")
              WT.put(j, TransactionInstance(cid,j,txni.rs ++ List[DataObject](DataObject(i)), txni.ws))              
            }
            
          }
          else if (op.toInt == 3){
            if (!txni.ws.map(_.oid).toSet.contains(i)){
//              println(s"tx $j has written object $i")
              WT.put(j, TransactionInstance(cid,j, txni.rs,txni.ws ++ List[DataObject](DataObject(i))))              
            }
          }          
        }
        case None => {
          // first time for this transaction instance
          val rs = ListBuffer[DataObject]()
          val ws = ListBuffer[DataObject]()          
          
          if (op.toInt == 1){
//            println(s"tx $j has read object $i")
            //add read to read set
            rs += DataObject(i)            
          }
          else if (op.toInt == 3){
//            println(s"tx $j has written object $i")
            // add update to write set
            ws += DataObject(i)
          }
          WT.put(j, TransactionInstance(cid,j,rs.toList, ws.toList))
        }
      }
     
    
    }}
    
    
    val WTres = WT.values.toList
        
    return (dataObjectsLabels.toList, txnLabels.toList, txnClassLabels.toList, WTres)
  }
  
//  def fill1_evarmInt(txni:TransactionFootprint, evarm:Array[Array[Int]]) = {
////    println("looking at "+txni)
//    val rset = txni.rs.map { x => x.oid }.toSet
//    val wset = txni.ws.map { x => x.oid }.toSet
//    val rdiff = rset.diff(wset)
//    val wdiff = wset.diff(rset)
////    println(rdiff)
////    println(wdiff)
////    val txid = txnlabels(txni.cid)
////    println(s"$txid: rset = $rset, wset = $wset, rdiff = $rdiff, wdiff = $wdiff")
//
//    
//    for (rse <- rdiff){
//      for (wse <- wset){
//        evarm(rse)(wse) = 1
//      }
//    }    
//  }
  
  def fill1_evarmInt(txni:TransactionInstance, evarm:Array[Array[Int]]) = {
//    println("looking at "+txni)
    val rset = txni.rs.map { x => x.oid }.toSet
    val wset = txni.ws.map { x => x.oid }.toSet
    val rdiff = rset.diff(wset)
    val wdiff = wset.diff(rset)
//    println(rdiff)
//    println(wdiff)
//    val txid = txnlabels(txni.cid)
//    println(s"${txni.cid}-${txni.ts}: rset = $rset, wset = $wset, rdiff = $rdiff, wdiff = $wdiff")

    
    for (rse <- rset){
      for (wse <- wset){
        evarm(rse)(wse) = 1
      }
    }    
  }
  
}



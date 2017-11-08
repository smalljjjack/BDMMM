package bigdata




import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Problem2 {
     
  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("hello").setMaster("local[*]")
    
    conf.set("spark.driver.allowMultipleContexts", "true");
    @transient
    val sc = new SparkContext(conf) 
    val P = sc.textFile(args(0)).map(_.split(",")).map(p => (p(0).toFloat, p(1).toFloat))
    val cell_size = 20
    val Total_cell = 250000
    val Row_cell = 500
    val Colomn_cell = 500
    val x_size = 10000.0
    val y_size = 10000.0
    def funcmap(arr:(Float,Float)):Seq[(Int,(Int,Int,Int))] = {
     val x = arr._1
     val y = arr._2
     val px = (x / cell_size + 1).toInt
     val py = (y / cell_size + 1).toInt
     if (x == x_size){        
        val px = (x / cell_size).toInt   
     }
     if (y == y_size){       
        val py = (y / cell_size).toInt
     }
     val Cell_No = px + (Colomn_cell - py) * Row_cell
     if (x<20 && y <20){
        val Index = List(Cell_No,Cell_No+1,Cell_No+Row_cell, Cell_No+Row_cell+1);

     for {
       i <- 0 until Index.length
     }
     yield {
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (x<20 && y>9980) {
        val Index = List(Cell_No,Cell_No+1,Cell_No-Row_cell,Cell_No-Row_cell+1);
     for {
       i <- 0 until Index.length
     }
     yield {
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (x>9980 && y>9980) {
        val Index = List(Cell_No,Cell_No-1,Cell_No-Row_cell,Cell_No-Row_cell-1);
     for {
       i <- 0 until Index.length
     }
     yield { 
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (x>9980 && y<20) {
        val Index = List(Cell_No,Cell_No-1,Cell_No+Row_cell,Cell_No+Row_cell-1);
     for {
       i <- 0 until Index.length
     }
     yield {  
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (y>20 && y<9980 && x<20) {
        val Index = List(Cell_No, Cell_No+1, Cell_No-Row_cell, Cell_No+Row_cell, Cell_No-Row_cell+1,  Cell_No+Row_cell+1);
     for {
       i <- 0 until Index.length
     }
     yield { 
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (y>20 && y<9980 && x>9980) {
        val Index = List(Cell_No,Cell_No-1, Cell_No-Row_cell,Cell_No+Row_cell,Cell_No-Row_cell-1, Cell_No+Row_cell-1);
     for {
       i <- 0 until Index.length
     }
     yield {   
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if ( x>20 && x<9980 && y<20) {
        val Index = List(Cell_No,Cell_No-1, Cell_No+1,Cell_No+Row_cell, Cell_No+Row_cell-1, Cell_No+Row_cell+1);
     for {
       i <- 0 until Index.length
     }
     yield {   
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else if (x>20 && x<9980 && y>9980) {
        val Index = List(Cell_No,Cell_No-1, Cell_No+1,Cell_No-Row_cell,Cell_No-Row_cell-1, Cell_No-Row_cell+1);
     for {
       i <- 0 until Index.length
     }
     yield {  
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           } else {
        val Index = List(Cell_No,Cell_No-1, Cell_No+1,Cell_No-Row_cell,Cell_No+Row_cell,Cell_No-Row_cell-1, Cell_No-Row_cell+1, Cell_No+Row_cell-1, Cell_No+Row_cell+1);
     for {
       i <- 0 until Index.length
     }
     yield {  
       if (i == 0){
       (Index(i),(1,0,Index.length-1))
       }else{
       (Index(i),(0,1,0))
       }
     };
           }
    }

    def funcreduce(a:(Int,Int,Int),b:(Int,Int,Int)):(Int,Int,Int) = {      
      if(a._1 >= 1){
        (a._1+b._1,a._2+b._2,a._3)
      }
      else{
        (a._1+b._1,a._2+b._2,a._3+b._3)
      }
    } 
    
    def funcmapvalue(arr:(Int,Int,Int)):Float ={
      val a = arr._1.toFloat
      val b = arr._2.toFloat
      val N = arr._3.toFloat
      if (b == 0.0){
        a
      }else{
        a/(b/N)
      }
    }
  val Pm=P.flatMap(funcmap)
  val Pmr= Pm.reduceByKey(funcreduce)

  
  val I0 = Pmr.mapValues(funcmapvalue)
  val I = I0.sortBy(_._2,false)
  val I_100 = sc.parallelize(I.take(100))
  I_100.saveAsTextFile("/home/yifan/spark/p2b")

  def getNB(c:Int):List[Int]={
    if(c == 1){
      val nb: List[Int] = List(2, 501, 502)
      return nb
    } else if (c == 500){
      val nb: List[Int] = List(499, 999, 1000)
      return nb
    } else if(c == 249501){
      val nb: List[Int] = List(249502, 249001, 249002)
      return nb
    } else if (c == 250000){
      val nb: List[Int] = List(249999, 249500, 249499)
      return nb
    } else if(c >= 2 && c <= 499){
      val nb: List[Int] = List(c-1, c+1, c+499, c+500, c+501)
      return nb
    } else if(c >= 501 && c != 249501 && (c-1)%500 == 0){
      val nb: List[Int] = List(c+1, c-500, c+500, c-499, c+501)
      return nb
    } else if(c >= 249502 && c <= 249999){
      val nb: List[Int] = List(c-1, c+1, c-501, c-500, c-499)
      return nb
    } else if(c >= 1000 && c <= 249500 && c%500 == 0){
      val nb: List[Int] = List(c-1, c-500, c+500, c-501, c+499)
      return nb
    } else {
      val nb: List[Int] = List(c-1, c+1, c-500, c+500, c+501, c-499, c-501, c+499)
      return nb
    }
  }
  val I1 = I_100.map(p=>(p._1,p._1))
  val I2 = I1.flatMapValues(p=>getNB(p))
  val I_original = I0.collectAsMap()
  val I3 = I2.map(p=>(p._1,(p._2,I_original(p._2))))
  val out = I3.groupByKey()
  out.saveAsTextFile("/home/yifan/spark/p2c")
  sc.stop()
  }
}

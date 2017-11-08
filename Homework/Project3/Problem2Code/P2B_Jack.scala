//get the cell number for each point
def get_cell_number_for_point(point: Array[String], row_length: Int = 500, cell_size: Int = 20): Int = {
    val x = point(0).toInt
    val y = point(1).toInt
    return (499 - y / cell_size) * row_length + x / cell_size + 1
}

//for each cell, get a list of number for its neighbor cells
def get_cell_neighbors(cell_number: Int, row_length: Int = 500): Array[Int] = {
    val top_neighbor_number = cell_number - row_length
    val bottom_neighbor_number = cell_number + row_length
    var neighbor_numbers = Array[Int](top_neighbor_number, bottom_neighbor_number)
    
    if ((cell_number - 1) % 500 != 0) {
        val left_neighbor_number = cell_number - 1
        val top_left_neighbor_number = top_neighbor_number - 1
        val bottom_left_neighbor_number = bottom_neighbor_number - 1
        neighbor_numbers = neighbor_numbers ++ Array[Int](left_neighbor_number,
                                                          top_left_neighbor_number,
                                                          bottom_left_neighbor_number)
    }
    
    if (cell_number % 500 != 0) {
        val right_neighbor_number = cell_number + 1
        val top_right_neighbor_number = top_neighbor_number + 1
        val bottom_right_neighbor_number = bottom_neighbor_number + 1
        neighbor_numbers = neighbor_numbers ++ Array[Int](right_neighbor_number,
                                                          top_right_neighbor_number,
                                                           bottom_right_neighbor_number)
    }

    return neighbor_numbers.filter(number => (number > 0 && number <= 250000))
}

//cell_region method is to create a structure of list((self_id,(self_density, neighbor_avg_density)))
//by using the flatMap, the list will be opened and become several tuple -> (self_id,(self_density, neighbor_avg_density))
def cell_region(cell_num_count:(Int, Int)):Seq[(Int,Tuple2[Int,Float])] = {
    var curr_cell_num = cell_num_count._1
    var curr_cell_count = cell_num_count._2
    var neighbors:Array[Int] = get_cell_neighbors(curr_cell_num)
    var seq:Seq[(Int,Tuple2[Int,Float])] = Seq((curr_cell_num,(curr_cell_count,0.toFloat)))
    for(i <- 0 to neighbors.size-1){
        var neighbor_size:Float = curr_cell_count/(get_cell_neighbors(neighbors(i)).size).toFloat
// regrad the neighbor(i) as output node, then the output will like this (neightbor(i)_ID,(0, curr_node_count/size of neightbor(i)'s neighbors))
        var tuple = (neighbors(i),(0, neighbor_size));
        seq = seq:+ tuple
    }
    return seq
}


def cell_density(x:(Int,Float)):Float = {
    if(x._2 == 0) return (x._1*10)
    return (x._1/x._2).toFloat
}
//x._1 is self_density, x._2 is avg_neighbors_density


def slef_neighbors_combine(x:(Int, Float)): Seq[(Int,(Tuple2[Int, Float],Seq[Tuple2[Int, Float]]))]={
    var curr_cell_num = x._1
    var curr_cell_count = x._2
    var neighbors:Array[Int] = get_cell_neighbors(curr_cell_num)
    var seq:Seq[(Int,(Tuple2[Int, Float],Seq[Tuple2[Int, Float]]))] = Seq((curr_cell_num,((curr_cell_num,curr_cell_count),Seq())))
    for(i <- 0 to neighbors.size-1) {
        var tuple = (neighbors(i),((neighbors(i),0.toFloat),Seq((curr_cell_num,curr_cell_count))))
        seq = seq:+ tuple
    }
    return seq
}
//create a structure of (slef_ID(slef_ID, slef_density),seq((neighbor(i),neighbor(i)_density),()...))

def slef_neighbor_combine_reducer(x:((Int, Float),Seq[(Int, Float)]), y:((Int, Float), Seq[(Int, Float)])):
    (Tuple2[Int, Float],Seq[Tuple2[Int, Float]])= {
    
    return((x._1._1, x._1._2+y._1._2),(x._2 ++ y._2)) 
//  x._2 is the seq/list which record the neightbor information, x._2 ++ y._2 will join the list
}
//for each record, it only contains the slef_density, or as a tuple inside of a seq which record as (neighbor_ID. neighbor_density)

val pointsRDD = sc.textFile("/home/mqp/Documents/Project3/Dataset/Points").
                   map(line => line.split(",").map(elem => elem.trim))

val cellPointsRDD = pointsRDD.map({point => (get_cell_number_for_point(point), 1)}).
                              reduceByKey((x, y) => x + y)
// cell pointsRDD do a count job, count how many points are in a same cell

val cellDensityFirstRDD = cellPointsRDD.flatMap(cell_num_count => cell_region(cell_num_count))
// cellDensityFirstRDD structure is that (slef_ID(slef_density, one neighbor_count/the size of neighbors))

val cellDensitySecondRDD = cellDensityFirstRDD.reduceByKey((x,y)=>(x._1 + y._1, x._2+y._2))
// cellDensityScondRDD's record is (slef_ID,(self_density, avg_neighbors_density))

val cellDensityThirdRDD = cellDensitySecondRDD.map({point=>(point._1, cell_density(point._2))})
//cellDensityTrirdRDD will get the odd of density for each cell

val problem2BRDD = cellDensityThirdRDD.sortBy(_._2, false)
//sort the cellDensityThirdRDD by _._2(odd of density)

problem2BRDD.take(100).foreach(println)
//print the answer for problem 2

val self_neighborComRDD = problem2BRDD.flatMap(slef_neighbor_density => slef_neighbors_combine(slef_neighbor_density))
// the structure of slef_neighborComRDD's record is (slef_ID,(slef_ID, slef_Density),List((neighbor_ID, neighbor_density),..))

val self_neightborCom_second_RDD = self_neighborComRDD.reduceByKey(slef_neighbor_combine_reducer)

val problem2CRDD = self_neightborCom_second_RDD.sortBy(_._2._1._2,false)
//Sort the self_neighborCom_second_RDD by slef_density

problem2CRDD.take(100).foreach(println)


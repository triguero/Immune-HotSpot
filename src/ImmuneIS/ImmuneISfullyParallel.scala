package ImmuneIS

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._
import math._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, ListBuffer}

/**
  * Created by pszit on 22/03/17.
  */
object ImmuneISfullyParallel {

  val INDEX_LATITUDE = 0
  val INDEX_LONGITUDE = 1
  val INDEX_BEARING = 2
  val INDEX_PARTIAL_LOCATION = 3
  val INDEX_FULL_LOCATION = 4     //Changed intentionally for experimental purpose..
  val INDEX_DATE = 5
  val INDEX_TIME = 6
  val INDEX_WEEKDAY = 7

  def main(arg: Array[String]) {

    // Check parameters and set level of Logs.

    Logger.getLogger("org").setLevel(Level.OFF)

    val logger = Logger.getLogger(this.getClass)
    if (arg.length < 5) {
      logger.error("=> wrong parameters number")
      System.err.println("Parameters \n\t<path-to-CSV> \n\t<Num of Partitions>\n\t<Percentage of Initial Points>\n\t<Mileage>\n\t<Output>")
      System.exit(1)
    }

    val dataFile = arg(0)
    val numPartitions = arg(1).toInt
    val PercentageInitialPoints = arg(2).toInt
    val mileage = arg(3).toDouble / 2.0
    // I divide this by 2 to keep the same number that in the table...
    val outputFile = arg(4)

    // Basic Setup
    var InitialTime = System.nanoTime
    val jobName = "Immune HotSpot Selection"
    val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> dataFile \"" + dataFile + "\"")
    logger.info("=> numPartitions \"" + numPartitions + "\"")
    logger.info("=> PercentageInitialPoints \"" + PercentageInitialPoints + "\"")
    logger.info("=> mileage \"" + mileage + "\"")

    val dataRawIndices = sc.textFile(dataFile: String, numPartitions).zipWithIndex().persist()

    // Defining the size of the supressor (or reduction set) as a percentage of the original set

    val InitialNumHotSpots = dataRawIndices.count() * PercentageInitialPoints / 100

    // Step 1: a percentage of random points is selected to form the suppressor set:

    val SuppresorSetIndices = dataRawIndices.takeSample(false, InitialNumHotSpots.toInt) // take Sample is an operation, directly returns data to driver as Array.

    println("The Suppressor Set is initialised with a number of elements: " + SuppresorSetIndices.size)

    // Step 1b: take out elements in the suppressor set from the dataRaw.
    val indexesSelected = SuppresorSetIndices.map(line => line._2).toSet

    val SetToBeReducedWithSupressors = dataRawIndices.filter {case (key, value) => !indexesSelected.contains(value) }.map(line => line._1).persist() // TODO: check whether this is the most efficient way to do it.

    println("SetToBeReducedWithSupressors: " + SetToBeReducedWithSupressors.count())

    // remove indexes from Suppressor

    val SuppressorSet = SuppresorSetIndices.map(line => line._1.split("\t")) // I split the data here already,

    var Stage1Initial = System.nanoTime


    // Step 2: remove redundancies within the Suppresor Set.

    val resultRedundancies = RemoveRedundancies(SuppressorSet, mileage)
    val ListOfRedundancies = resultRedundancies._1.toSet
    val fitnessSuppressorSet = resultRedundancies._2

    //println(ListOfRedundancies)

    var suppressedGuys = 0
    for (i <- 0 until fitnessSuppressorSet.size) suppressedGuys += fitnessSuppressorSet(i)
    //  print(fitnessSuppressorSet(i)+",")
    println("I have suppressed this number of guys: " + suppressedGuys)

    val CleanedSuppresorSet = SuppressorSet.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)
    val CleanedFitnessSuppressorSet = fitnessSuppressorSet.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)

    println("Suppressor Set after cleaning of redundant points: " + CleanedSuppresorSet.size)

    println("Time to Remove redundant points in the Suppresor set : "+(System.nanoTime - InitialTime)/ 1e9)

    val Stage1time = (System.nanoTime - Stage1Initial)/ 1e9


    var step3time = System.nanoTime

    // Steps 3 and 4: Compute the distance between the set of points to be reduced and the Suppressor Set.

    // we broadcast this suppresor set to all the nodes:

    val SuppressorSet_broadcast = sc.broadcast(CleanedSuppresorSet)

    var result = SetToBeReducedWithSupressors.mapPartitionsWithIndex((index, dataset) => EliminateAndFitness(dataset, SuppressorSet_broadcast, mileage, index)).persist() // you need to persist because i'm going to be using this for  a while

    var data = result.mapPartitions(info => getData(info)).persist()
  //  var fitness_alt2 = result.mapPartitions(info => getFitness(info)).reduceByKey(combine).map(line => line._2).collect()(0)

    val alt2 =  result.mapPartitions(info => getFitness(info)).collect()
    var fitness_alt2 = getFitnessArray(alt2)

    if(fitness_alt2.size != CleanedFitnessSuppressorSet.size){println("ERROR!!!"); System.exit(-1)}
    // aggregate with previous fitness:
    var suppressedSoFar = 0
    for(i<-0 until fitness_alt2.size){
      CleanedFitnessSuppressorSet(i) += fitness_alt2(i)
      suppressedSoFar +=  CleanedFitnessSuppressorSet(i)
    }

    data.unpersist()
    result.unpersist()

    println("SuppressedSoFar "+ suppressedSoFar)

    println("Time to Remove redundant points in SetToBeReduced : "+(System.nanoTime - step3time)/ 1e9)

    val Stage2time = (System.nanoTime - step3time)/ 1e9


    var lastStepTime = System.nanoTime

    // STEP 5: eliminate redundancies in the CellsToBeDeleted IN PARALLEL!!

    /// this is going to be done in two phases..

    // 1st : we clean each partition separately...

    println("Applying Step 5.1")
    var resultStep5 = data.mapPartitions(dataset => CleanEachMap(dataset, mileage)).persist()  // Now in each partition, I have the remaining Cells + their fitness...

    // 2n we compare 1 vs. 2..N, 2 vs 3..N, etc...

    println("Applying Step 5.2")

    var fitnessStep5 = new ArrayBuffer[Int]

    for (i <- 0 until resultStep5.getNumPartitions) {


      var subset = resultStep5.filterByRange(i, i).collect()(0)._2._1 // This a partition, I take only the data.. don't need the fitness here.

      val subset_broadcast = sc.broadcast(subset)

      println("Subset: "+subset.size)

      var newData = resultStep5.mapPartitionsWithIndex((index, info) => Cleaning(info, subset_broadcast, mileage,i)).persist()

      // I now need to aggregate the fitness achieved in every partition for the current subset.

      val step5 =  newData.mapPartitions(info => getFitnessFrom3Tuple(info)).collect()
      var fitness_temp = getFitnessArray(step5)

      println("fitness size: "+fitness_temp.size)
      fitnessStep5 ++= fitness_temp

      resultStep5 = newData.mapPartitions(info => getDataAndFitnessFrom3Tuple(info)).persist()

    }

    var dataToBeCombined = resultStep5.collect()

    println("Time to Remove redundant points in remaining CellsToBeRemoved : "+(System.nanoTime - lastStepTime)/ 1e9)

    val Stage3time = (System.nanoTime - lastStepTime)/ 1e9


    data.unpersist()
    var outString = new ArrayBuffer[String]

    for (i<-0 until dataToBeCombined.size){
     // println("2. dataToBeCombined(i)._2 size: " +dataToBeCombined(i)._2.size)

      outString ++= dataToBeCombined(i)._2._1
    }

    var CellsToBeEliminated = outString.toArray

    println("CellsToBeEliminated size: "+CellsToBeEliminated.size)
    println("fitnessStep5 size: "+fitnessStep5.size)

    if(CellsToBeEliminated.size != fitnessStep5.size){println("ERROR in Step 5!!!"); System.exit(-1)}

    var fitnessLastRound = CleanedFitnessSuppressorSet

    var numSuppressedGuysByRemainingCells = 0

    for(i<-0 until fitnessStep5.size){
     // print(fitnessStep5(i)+";")
      numSuppressedGuysByRemainingCells += fitnessStep5(i)
    }
    println("\n numSuppressedGuysByRemainingCells: "+numSuppressedGuysByRemainingCells)


    var resultingSuppressorSet=  if (CellsToBeEliminated.size > 0){
      fitnessLastRound = CleanedFitnessSuppressorSet ++ fitnessStep5 // Join fitness

      var CellsChunks = CellsToBeEliminated.map(line => line.split("\t"))

      CleanedSuppresorSet ++ CellsChunks
    }else{
      CleanedSuppresorSet
    }

    println("The resultingSuppressorSet Size is:" + resultingSuppressorSet.size )

    // Step 6: remove those Suppressorcells with fitness == 0.

    val finalSuppressorSet = resultingSuppressorSet.zipWithIndex.filter{case(key, value) => fitnessLastRound(value.toInt)!=0 }.map(line => line._1)

    println("The finalSuppressorSet Size is:" + finalSuppressorSet.size )


    // compute final fitnessSuppressorSet
    val finalFitnessSuppressorSet = fitnessLastRound.filter(_!=0)


    // to double-check everything went well
    if(finalFitnessSuppressorSet.size != finalSuppressorSet.size){println("ERROR!!!"); System.exit(-1)}

    var runtime = (System.nanoTime - InitialTime)/ 1e9
    println ("Runtime: "+ runtime)

    // Write Output:
    var outPut = ""

    var totalNumberOfSuppressedGuys = 0
    for(i<-0 until finalSuppressorSet.size){
      //outPut += (finalSuppressorSet(i).mkString("\t")) +"\n"
      outPut += finalSuppressorSet(i)(0) + "\t" + finalSuppressorSet(i)(1) + "\t" + finalFitnessSuppressorSet(i).toString() + "\n" // I only print the necessary info.
      totalNumberOfSuppressedGuys += finalFitnessSuppressorSet(i)
    }
    //print(outPut)

    println("We have suppressed: "+totalNumberOfSuppressedGuys)

    var pw = new PrintWriter(new File(outputFile))
    pw.write(outPut)
    pw.close()


    // for printing, we only take those suppressors with fitness > 10

    val SSgreater10 = finalSuppressorSet.zipWithIndex.filter{case(key, value) => finalFitnessSuppressorSet(value.toInt)>10 }.map(line => line._1)
    println("The SSgreater10 Size is:" + SSgreater10.size )
    val fitnessSSgreater10 = finalFitnessSuppressorSet.filter(_>10)

    outPut = ""

    for(i<-0 until SSgreater10.size){
      outPut += SSgreater10(i)(0) + "\t" + SSgreater10(i)(1) + "\t" + fitnessSSgreater10(i).toString() + "\n" // I only print the necessary info.
    }


    pw = new PrintWriter(new File(outputFile+"_grt10"))
    pw.write(outPut)
    pw.close()

    println("****************************************\n")
    println(resultingSuppressorSet.size + "\t" + finalSuppressorSet.size+"\t"+ SSgreater10.size + "\t"+ runtime+ "\t"+ Stage1time + "\t"+ Stage2time + "\t"+ Stage3time)

    System.exit(1)
  }


  // functions to extract relevant info (map partition + data) info from the previous stage EliminateAndFitness
  // just a transformatino!!!
  def getData[T](iter: Iterator[(Int, (Array[String], Array[Int]))]):Iterator[(Int, Array[String])] = {
    var data = iter.next()
    var result = new Array[(Int,Array[String])](1)
    result(0) = (data._1,data._2._1)
    result.iterator
  }

  def getFitness[T](iter: Iterator[(Int, (Array[String], Array[Int]))]):Iterator[(Int, Array[Int])] = {
    var data = iter.next()
    var result = new Array[(Int,Array[Int])](1)
    result(0) = (data._1,data._2._2)
    result.iterator
  }



  def getFitnessFrom3Tuple[T](iter: Iterator[(Int, (Array[String], Array[Int]), Array[Int])]):Iterator[(Int, Array[Int])] = {
    var data = iter.next()
    var result = new Array[(Int,Array[Int])](1)
    result(0) = (data._1,data._3)
    result.iterator
  }

  def getDataFrom3Tuple[T](iter: Iterator[(Int, (Array[String], Array[Int]), Array[Int])]):Iterator[(Int, Array[String])] = {
    var data = iter.next()
    var result = new Array[(Int,Array[String])](1)
    result(0) = (data._1,data._2._1)
    result.iterator
  }

  def getDataAndFitnessFrom3Tuple[T](iter: Iterator[(Int, (Array[String], Array[Int]), Array[Int])]):Iterator[(Int, (Array[String], Array[Int]))] = {
    var data = iter.next()
    var result = new Array[(Int, (Array[String], Array[Int]))](1)
    result(0) = (data._1,data._2)
    result.iterator
  }



  /**
    * Aggregate the fitnes of several map partitions.
    * @param mapOut
    * @return
    */

  def getFitnessArray(mapOut: Array[(Int, Array[Int])]): Array[Int] ={

   // var mapOut = newData.mapPartitions(info => getFitness(info)).collect()
    var size = mapOut.size
    var sizeFitness = mapOut(0)._2.size

    var fitnessPartial = new Array[Int](sizeFitness)
    for(m<-0 until size){
      for(z<-0 until sizeFitness){
        val fitness= mapOut(m)._2(z)
        fitnessPartial(z) +=fitness
      }
    }

    fitnessPartial
  }


  /**
    * Function to clean an RDD....
    * @param iter
    * @param subset
    * @param mileage
    * @tparam T
    * @return
    */

  def Cleaning[T](iter: Iterator[(Int, (Array[String], Array[Int]))], subset:  Broadcast[Array[String]], mileage: Double, mapBeingProcess: Int): Iterator[(Int, (Array[String], Array[Int]), Array[Int])] = {

    var cont = 0

    var data = iter.next() // this iterator will have ONLY ONE ELEMENT, if this function is used correctly after running EliminateAndFitness
    if (iter.hasNext) {
      println("ERROR, this couldn't happen"); System.exit(-1)
    }

    var chunkBC = subset.value // the chunk of data coming from the driver (same for every execution of this function).

    // the piece of data of this map that needs reducing
    var mapData = data._2
    var fitnessCurrentPartition = data._2._2
    var MapNumber = data._1

    val indicesToBeRemoved = HashSet.empty[Int]
    var sizeChunkBC = chunkBC.size
    var sizeMapData = mapData._1.size

    var fitnessGainBC = new Array[Int](sizeChunkBC) // here i will aggregate the fitness gain in every instance of the chunk coming in the broadcast...

    var toBeRemoved = new ListBuffer[Boolean]

    if (mapBeingProcess < MapNumber) { // IMPORTANT IF...to implement comparison Current Partition (subset), against 'higher' parititons.

      for (i <- 0 until sizeChunkBC) {

          val instance1 = chunkBC(i).split("\t")

          breakable {
            for (j <- 0 until sizeMapData) {

              if (!indicesToBeRemoved.contains(j)) {
                // I only go ahead if this instance hasn't been marked to be removed.
                  // Checking this is not the same example... MIGHT happen in this implemetnation.
                val instance2 = mapData._1(j).split("\t")
                val miles = distanceHotSpot(instance1, instance2)

                if (miles != -1) {
                     if (miles <= mileage) {
                      val bearing_diff = scala.math.abs(instance1(INDEX_BEARING).toInt - instance2(INDEX_BEARING).toInt)
                      val module = (bearing_diff + 180) % 360 - 180
                      val delta = scala.math.abs(module)

                      if (delta < 60) {
                        indicesToBeRemoved += j
                        fitnessGainBC(i) += fitnessCurrentPartition(j)+1
                        break

                      }
                    }
                }
              }
            }
          } // end breakable

      }

      println("I am going to remove in this map " + MapNumber + "(" + sizeChunkBC + "): " + indicesToBeRemoved.size )

    }

// Curiously, what I send back now is:  the remaining Cells with their fitness, + the Fitness achieve by the subset we broadcast before!
    var result = new Array[(Int, (Array[String], Array[Int]), Array[Int])](1)
    // we have to filter both the instances and the fitness
    val dataFiltered = mapData._1.zipWithIndex.filterNot { case (key, value) => indicesToBeRemoved.contains(value.toInt) }.map(line => line._1)
    val fitnessFiltered = mapData._2.zipWithIndex.filterNot { case (key, value) => indicesToBeRemoved.contains(value.toInt) }.map(line => line._1)

    result(0) = (MapNumber, (dataFiltered,fitnessFiltered), fitnessGainBC)
    result.iterator
  }


  /**
    * Function to perform the first stage of STep 5, cleaning each map separately
    * @param iter
    * @param mileage
    * @tparam T
    * @return
    */

  def CleanEachMap[T](iter: Iterator[(Int, Array[String])], mileage: Double): Iterator[(Int, (Array[String], Array[Int]))] = {
    var data = iter.next() // this iterator will have ONLY ONE ELEMENT, if this function is used correctly after running EliminateAndFitness
    if (iter.hasNext) {
      println("ERROR, this couldn't happen"); System.exit(-1)
    }

    var MapNumber = data._1
    var mapData = data._2.map(line => line.split("\t"))

    var step5 = RemoveRedundancies(mapData, mileage)
    val ListOfRedundancies = step5._1.toSet
    var fitnessStep5 = step5._2

    var fitnessCellsToBeRemoved = fitnessStep5.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)
    var CellsToBeEliminated = mapData.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1.mkString("\t"))

   // println("CleanEachMap removing " + MapNumber + ": " + ListOfRedundancies.size)


    var result = new Array[(Int, (Array[String], Array[Int]))](1)
    result(0) = (MapNumber, (CellsToBeEliminated, fitnessCellsToBeRemoved))
    result.iterator
  }





    /**
    * the distance between two points defined by longitude and latitued is given by the haversine formula.
    * initially, I am going to do this locally, but maybe it should be also parallel (quite complicated as this is a greedy operation)
    * This fucntion will return the indices of redundant instances, and the FITNESS of the suppressors
    *
    * @param SuppressorSet
    * @param MILEAGE
    * @return
    */


  def RemoveRedundancies (SuppressorSet: Array[Array[String]], MILEAGE: Double): Pair[HashSet[Int],Array[Int]] = {

    val size = SuppressorSet.size
    //  val Indexes
   /// val indicesToBeRemoved = new ListBuffer[Int]
   val indicesToBeRemoved = HashSet.empty[Int]


    var fitnessSuppressor = new Array[Int](size)

//    var time = System.nanoTime

    var latitudeVector = new  Array[Double](size)
    var longitudeVector = new  Array[Double](size)


    for (i <- 0 until size) {
      latitudeVector(i) = SuppressorSet(i)(INDEX_LATITUDE).toDouble
      longitudeVector(i) = SuppressorSet(i)(INDEX_LONGITUDE).toDouble
    }

    for (i <- 0 until size) {

     // if(i%1000 == 0) println ((System.nanoTime - time)/ 1e9)

        var fitness = 0

        for( j<-i+1 until size){

          if(!indicesToBeRemoved.contains(j)) { // I only go ahead if this instance hasn't been marked to be removed.   // TODO: Check with Grazziela
            // IF LOCATION AND WEEKDAY MAKE SENSE.. WE CONTINUE.. OTHERWISE.. WE DON'T EVEN COMPARE:
            if (SuppressorSet(i)(INDEX_PARTIAL_LOCATION).equalsIgnoreCase(SuppressorSet(j)(INDEX_PARTIAL_LOCATION))) {
         //     if (SuppressorSet(i)(INDEX_WEEKDAY).equalsIgnoreCase(SuppressorSet(j)(INDEX_WEEKDAY))) {

                // I'M GOING TO IGNORE THE TIME FOR NOW.

               // val miles = Haversine.haversine(SuppressorSet(i)(INDEX_LATITUDE).toDouble, SuppressorSet(i)(INDEX_LONGITUDE).toDouble, SuppressorSet(j)(INDEX_LATITUDE).toDouble, SuppressorSet(j)(INDEX_LONGITUDE).toDouble)
               val miles = Haversine.haversine(latitudeVector(i), longitudeVector(i), latitudeVector(j), longitudeVector(j))

                if (miles <= MILEAGE) {
                  val bearing_diff = scala.math.abs(SuppressorSet(i)(INDEX_BEARING).toInt - SuppressorSet(j)(INDEX_BEARING).toInt)
                  val module = (bearing_diff + 180) % 360 - 180
                  val delta = scala.math.abs(module)

                  // println("delta: " + delta)
                  if (delta < 60) {
                    indicesToBeRemoved += j
                    fitness += 1;
                  }
                }

           //   }
            }
          }

        }  // End for j, to check pair with Cell_i


        fitnessSuppressor(i) = fitness
     }

    println("I will remove a number of redundant suppressors: "+indicesToBeRemoved.size)

    (indicesToBeRemoved,fitnessSuppressor)
 }


   def distanceHotSpot (instance1: Array[String], instance2: Array[String]): Double = {
     var miles = -1.0

     if (instance1(INDEX_PARTIAL_LOCATION).equalsIgnoreCase(instance2(INDEX_PARTIAL_LOCATION))) {
   //    if (instance1(INDEX_WEEKDAY).equalsIgnoreCase(instance2(INDEX_WEEKDAY))) {
         miles = Haversine.haversine(instance1(INDEX_LATITUDE).toDouble, instance1(INDEX_LONGITUDE).toDouble, instance2(INDEX_LATITUDE).toDouble, instance2(INDEX_LONGITUDE).toDouble)
     //  }
     }

    miles
   }


  /**
    * Function for Step 3.
    * @param iter
    * @param Suppressor
    * @param mileage
    * @param idMap
    * @tparam T
    * @return
    */

  def EliminateAndFitness[T](iter: Iterator[String], Suppressor: Broadcast[Array[Array[String]]], mileage: Double, idMap: Int): Iterator[(Int, (Array[String], Array[Int]))] = {

    val (iterator1, iterator2) = iter.duplicate



    var toBeRemoved = new ListBuffer[Boolean]

    var fitnessSuppressor = new Array[Int](Suppressor.value.size)

    //println("Suppressor.value.size: "+Suppressor.value.size)
    var cont = 0
    var cont2= 0
    //var cont3 = 0

    // Here, we have to do it the other way around, for each instance in the SetTOBeREduced against the Suppressor Set.
    while(iterator1.hasNext){
      val instance1 = iterator1.next().split("\t")

      //println(instance1.mkString(" "))

      var found = false
      // I compare every instance from the SetToBeReducedWithSuppressors against the Suppressor set that came as a broadcast, aiming to find the closest one.

      breakable {
        for (i <- 0 until Suppressor.value.size) {
          val miles = distanceHotSpot(instance1, Suppressor.value(i))

          //println("miles: "+miles)
          if (miles != -1) {
            // -1 would mean that there is bearing or something preventing this point from being removed.

            if (miles <= mileage) {
              val bearing_diff = scala.math.abs(instance1(INDEX_BEARING).toInt - Suppressor.value(i)(INDEX_BEARING).toInt)
              val module = (bearing_diff + 180) % 360 - 180
              val delta = scala.math.abs(module)

              // println("delta: " + delta)
              if (delta < 60) {
                found = true
                fitnessSuppressor(i) += 1
             //   cont3 +=1
                break // TODO: check with Grazziela

              }
            }
          }
        }
      }
      // If I have found a suppressor cell close by, it means this is redundant and can be eliminated.
      if(found){
        cont += 1
        toBeRemoved += true
        // println("Need to be removed: "+MinDistance)
      }else{
        toBeRemoved += false
      }
      cont2 += 1

    } // End while loop for instances in this MAP.

  //  println("I have decided to remove a guy a number of times: "+cont3)
    // Here I filter those spots that have been marked as redundant.

    println("I'm going to eliminate: "+cont + "; out of: "+cont2)

    //println("iterator2 size : "+iterator2.length)

    val filteredSet = iterator2.zipWithIndex.filter{case (key, value) => !toBeRemoved(value.toInt)}

    // Create directly the set to be returned from here.

  //  println("Filtered Set : "+filteredSet.size)
   // val noRedundantData = HashSet.empty[Array[String]]
   val noRedundantData = HashSet.empty[String]
    while(filteredSet.hasNext){
      var guy = filteredSet.next()._1
      noRedundantData += guy
    }


    var result = new Array[(Int,(Array[String],Array[Int]))](1)

    result(0) =  (idMap, (noRedundantData.toArray,fitnessSuppressor))

    result.iterator
  }



  def combineEliminationAndFitness(mapOut: Array[(Int, (Array[String], Array[Int]))] ): Pair[Array[String],Array[Int]] ={

    var size = mapOut.size
    var outString = new ArrayBuffer[String]

    var sizeFitness = mapOut(0)._2._2.size
    var fitnessSuppressor = new Array[Int](sizeFitness)

   // println("size: "+size)
    // println("sizeFitness: "+sizeFitness)

    for(i<-0 until size){
     // println("mapOut "+ i + "; "+mapOut(i)._2._1.size)
      outString ++= mapOut(i)._2._1

      for(j<-0 until sizeFitness){
        fitnessSuppressor(j) += mapOut(i)._2._2(j)
      }

    }

    println("Size of the remaining examples in SetToBeReduced: "+outString.size)
    (outString.toArray, fitnessSuppressor)

  }
  /**
    * Haversine is necessary to compute distance between GPS coordinates.
    */
  object Haversine {
    val R = 6372.8  //radius in km

    def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians

      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    }

  }

  }


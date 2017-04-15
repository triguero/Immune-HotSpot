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
object ImmuneIS {

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
    val mileage = arg(3).toDouble / 2.0  // I divide this by 2 to keep the same number that in the table...
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

    // Step 1: a percenage of random points is selected to form the suppressor set:

    val SuppresorSetIndices = dataRawIndices.takeSample(false, InitialNumHotSpots.toInt)  // take Sample is an operation, directly returns data to driver as Array.
 //   val SuppresorSetIndices = dataRawIndices.sample(false, InitialNumHotSpots.toInt)

    println("The Suppressor Set is initialised with a number of elements: "+SuppresorSetIndices.size)

   // val SuppressorSet_broadcast = sc.broadcast(SuppresorSetIndices)

    // Step 1b: take out elements in the suppressor set from the dataRaw.
    val indexesSelected = SuppresorSetIndices.map(line => line._2).toSet

    val SetToBeReducedWithSupressors = dataRawIndices.filter{case (key, value) => !indexesSelected.contains(value)}.map(line => line._1).persist()

    println("SetToBeReducedWithSupressors: "+SetToBeReducedWithSupressors.count())

    // remove indexes from Suppressor

    val SuppressorSet = SuppresorSetIndices.map(line => line._1.split("\t")) // I split the data here already,

/*
    for(i<-0 until SuppressorSet.size){
      if(SuppressorSet(i)(INDEX_PARTIAL_LOCATION).equalsIgnoreCase("Brian Clough Way")){
        println("I selected this in the original suppressor Set "+ SuppressorSet(i).mkString(" ") )
      }
    }
*/

    // Step 2: remove redundancies within the Suppresor Set.

    var Stage1Initial = System.nanoTime

    val resultRedundancies = RemoveRedundancies(SuppressorSet, mileage)
    val ListOfRedundancies = resultRedundancies._1.toSet
    val fitnessSuppressorSet = resultRedundancies._2


    //println(ListOfRedundancies)

    var suppressedGuys=0
    for(i<-0 until fitnessSuppressorSet.size) suppressedGuys+= fitnessSuppressorSet(i)
    //  print(fitnessSuppressorSet(i)+",")
    println("I have suppressed this number of guys: "+suppressedGuys)

    val CleanedSuppresorSet = SuppressorSet.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)
    val CleanedFitnessSuppressorSet = fitnessSuppressorSet.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)

    println("Suppressor Set after cleaning of redundant points: "+CleanedSuppresorSet.size)

    println("Time to Remove redudant points in the Suppresor set : "+(System.nanoTime - Stage1Initial)/ 1e9)

    val Stage1time = (System.nanoTime - Stage1Initial)/ 1e9

    var step3time = System.nanoTime


    // Steps 3 and 4: Compute the distance between the set of points to be reduced and the Suppressor Set.
    // It's kind of an special 1-NN that will directly remove redundant intances

    // we broadcast this suppresor set to all the nodes:

    /*for(i<-0 until CleanedFitnessSuppressorSet.size){
      if(CleanedSuppresorSet(i)(INDEX_PARTIAL_LOCATION).equalsIgnoreCase("Brian Clough Way")){
        println("I selected this in the cleaned suppressor Set "+ CleanedSuppresorSet(i).mkString(" ") + "; fitness: "+ CleanedFitnessSuppressorSet(i))
      }
    }
*/

   // System.exit(1)

    val SuppressorSet_broadcast = sc.broadcast(CleanedSuppresorSet)

  //   val ReducedSet = SetToBeReducedWithSupressors.mapPartitions(dataset => Special1NN(dataset, SuppressorSet_broadcast, mileage)).collect()
    val alt2 = SetToBeReducedWithSupressors.mapPartitionsWithIndex((index, dataset) => EliminateAndFitness(dataset, SuppressorSet_broadcast, mileage, index)).collect()

    val results_alt2 = combineEliminationAndFitness(alt2)
    val ReducedSet = results_alt2._1
    val fitness_alt2 = results_alt2._2


    if(fitness_alt2.size != CleanedFitnessSuppressorSet.size){println("ERROR!!!"); System.exit(-1)}

    var suppressedSoFar =0
    // aggregate with previous fitness:
    for(i<-0 until fitness_alt2.size){
      CleanedFitnessSuppressorSet(i) += fitness_alt2(i)
      suppressedSoFar +=  CleanedFitnessSuppressorSet(i)

    /*  if(CleanedSuppresorSet(i)(INDEX_PARTIAL_LOCATION).equalsIgnoreCase("Brian Clough Way")){
       println("I am incrementing the fitness of "+ CleanedSuppresorSet(i).mkString(" ")+ " by "+fitness_alt2(i))
        // println("I selected this in the cleaned suppressor Set "+ CleanedSuppresorSet(i).mkString(" ") + "; fitness: "+ CleanedFitnessSuppressorSet(i))
      }
*/

      //  print(CleanedFitnessSuppressorSet(i)+",")
    }

    println("SuppressedSoFar "+ suppressedSoFar)

    println("Time to Remove redundant points in SetToBeReduced : "+(System.nanoTime - step3time)/ 1e9)

    val Stage2time = (System.nanoTime - step3time)/ 1e9


    var lastStepTime = System.nanoTime

   // System.exit(1)
    // Steps 5 +  2 if necessary.

   var fitnessStep5: Array[Int] = null
   var CellsToBeEliminated: Array[Array[String]]= null

   if(ReducedSet.size>0){
      println("Applying step 5: removing redundancies in CellstoBeEliminated !!")

      var ReducedSet2 = ReducedSet.map(line => line.split("\t"))  // need to split this for removing the redundant points

      val resultRedundancies_2 = RemoveRedundancies(ReducedSet2, mileage)
      val ListOfRedundancies = resultRedundancies_2._1.toSet
      var fitnessTemp = resultRedundancies_2._2


      fitnessStep5 = fitnessTemp.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)
      CellsToBeEliminated = ReducedSet2.zipWithIndex.filterNot(line => ListOfRedundancies.contains(line._2)).map(line => line._1)

   }

    println("Time to Remove redundant points in remaining CellsToBeRemoved : "+(System.nanoTime - lastStepTime)/ 1e9)

    val Stage3time = (System.nanoTime - lastStepTime)/ 1e9


    var numSuppressedGuysByRemainingCells = 0

    for(i<-0 until fitnessStep5.size){
     // print(fitnessStep5(i)+";")
      numSuppressedGuysByRemainingCells += fitnessStep5(i)
    }
    println("\n numSuppressedGuysByRemainingCells: "+numSuppressedGuysByRemainingCells)

    // Suppressor cells <- Suppressors Cells + CellsToBeEliminated.
    var fitnessLastRound = CleanedFitnessSuppressorSet

    var resultingSuppressorSet=  if (CellsToBeEliminated.size > 0){
      fitnessLastRound = CleanedFitnessSuppressorSet ++ fitnessStep5 // Join fitness
      CleanedSuppresorSet ++ CellsToBeEliminated
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


    /*  if(finalSuppressorSet(i)(INDEX_PARTIAL_LOCATION).equalsIgnoreCase("Brian Clough Way")){
        println("Final fitness of "+ finalSuppressorSet(i).mkString(" ")+ " is "+finalFitnessSuppressorSet(i))
        // println("I selected this in the cleaned suppressor Set "+ CleanedSuppresorSet(i).mkString(" ") + "; fitness: "+ CleanedFitnessSuppressorSet(i))
      }*/

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

      if(!indicesToBeRemoved.contains(i)) {
        for (j <- (i + 1) until size) {

          if (!indicesToBeRemoved.contains(j)) {
            // I only go ahead if this instance hasn't been marked to be removed.
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

        } // End for j, to check pair with Cell_i

      }
      fitnessSuppressor(i) = fitness
    }

    println("I will remove a number of redundant suppressors: "+indicesToBeRemoved.size)

    (indicesToBeRemoved,fitnessSuppressor)
  }


   def distanceHotSpot (instance1: Array[String], instance2: Array[String]): Double = {
     var miles = -1.0


     if (instance1(INDEX_PARTIAL_LOCATION).equalsIgnoreCase(instance2(INDEX_PARTIAL_LOCATION))) {
    //   if (instance1(INDEX_WEEKDAY).equalsIgnoreCase(instance2(INDEX_WEEKDAY))) {
         miles = Haversine.haversine(instance1(INDEX_LATITUDE).toDouble, instance1(INDEX_LONGITUDE).toDouble, instance2(INDEX_LATITUDE).toDouble, instance2(INDEX_LONGITUDE).toDouble)
      // }
     }

    miles
   }





  def EliminateAndFitness[T](iter: Iterator[String], Suppressor: Broadcast[Array[Array[String]]], mileage: Double, idMap: Int): Iterator[(Int, (Array[String], Array[Int]))] = {

    val (iterator1, iterator2) = iter.duplicate

  //  val (iterator3, iterator4) = iterator2.duplicate

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

            /*
            if(instance1(INDEX_PARTIAL_LOCATION).equalsIgnoreCase("Brian Clough Way")){
              println("Here it's a match")
              println(instance1.mkString(" ")+ "\t" + Suppressor.value(i).mkString(" ") + "\t miles: "+miles)
            }
            */


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

   // println("iterator2 size : "+iterator2.length)

    val filteredSet = iterator2.zipWithIndex.filter{case (key, value) => !toBeRemoved(value.toInt)}

    // Create directly the set to be returned from here.

  //  println("Filtered Set : "+filteredSet.size)
    val noRedundantData = ListBuffer.empty[String]
    while(filteredSet.hasNext){
      var guy = filteredSet.next()._1
     // println(guy)
      noRedundantData += guy
    }

    // println("noRedundantData.size: "+noRedundantData.size)

   /* var suppressedGuys=0
    for(i<-0 until fitnessSuppressor.size) suppressedGuys+= fitnessSuppressor(i)

    println("Suppressed guys in map "+idMap+": "+suppressedGuys)
*/

    var result = new Array[(Int,(Array[String],Array[Int]))](1)

    result(0) =  (idMap, (noRedundantData.toArray,fitnessSuppressor))

    result.iterator
  }


  /*
    * TODO: come up with a parallel version of the remove reduncies function...
    * @param MILEAGE
    * @return

  def RemoveRedundanciesInParallel (iter: Iterator[String], MILEAGE: Double): Pair[HashSet[Int],Array[Int]] = {

    val (iterOriginal, iterForFinalCleaning) = iter.duplicate // iter and iterOriginal are the same... iterFor FinalCleaning will be use at the end for cleaning.

    val indicesToBeRemoved = HashSet.empty[Int]


    var i = 0

    while(iter.hasNext){ // for each instance...

      val instance1 = iter.next().split("\t") // I read the current example...

      val (iterator1, iterator2) = iter.duplicate   // iter will be equal to iterator1, iterator2 is a new iterator.

      var fitness = 0

      var j = i+1

      while(iterator2.hasNext){ // I skipped the same element, as I read it already .. (this implements the for loop j<-i+1 until size.

        val instance2 = iterator2.next().split("\t")

        if(!indicesToBeRemoved.contains(j)) { // I only go ahead if this instance hasn't been marked to be removed.
          // IF LOCATION AND WEEKDAY MAKE SENSE.. WE CONTINUE.. OTHERWISE.. WE DON'T EVEN COMPARE:
          if (instance1(INDEX_FULL_LOCATION).equalsIgnoreCase(instance2(INDEX_FULL_LOCATION))) {
            if (instance1(INDEX_WEEKDAY).equalsIgnoreCase(instance2(INDEX_WEEKDAY))) {

              val miles = Haversine.haversine(instance1(INDEX_LATITUDE).toDouble, instance1(INDEX_LONGITUDE).toDouble, instance2(INDEX_LATITUDE).toDouble, instance2(INDEX_LONGITUDE).toDouble)

              if (miles <= MILEAGE) {
                val bearing_diff = scala.math.abs(instance1(INDEX_BEARING).toInt - instance2(INDEX_BEARING).toInt)
                val module = (bearing_diff + 180) % 360 - 180
                val delta = scala.math.abs(module)

                // println("delta: " + delta)
                if (delta < 60) {
                  indicesToBeRemoved += j
                  fitness += 1;
                }
              }

            }
          }
        }


        j += 1
      }



        i += 1
    } // end main loop


  }
    */

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
    val R = 6372.8/1.609344  //radius in miles!!

    def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians

      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    }

  }

  }


/*  OLD CODE:
  /**
    * Method that performs the immune selection, here I only apply the distance without considering bearing..
    * It will return a filtered Set of points that are not redundant w.r.t. the SuppressorSet
    * @param iter
    * @tparam T
    * @return
    */
  def Special1NN[T](iter: Iterator[String], Suppressor: Broadcast[Array[Array[String]]], mileage: Double): Iterator[String] = {

    val (iterator1, iterator2) = iter.duplicate

    var toBeRemoved = new ListBuffer[Boolean]

    var fitnessSuppressor = new Array[Int](Suppressor.value.size)

    var cont = 0
    var cont2= 0
    // Here, we have to do it the other way around, for each instance in the SetTOBeREduced against the Suppressor Set.
    while(iter.hasNext){
      val instance1 = iter.next().split("\t")

      //println(instance1.mkString(" "))

      var MinDistance = -1.0
      // var nearest = -1
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
                MinDistance = miles
                fitnessSuppressor(i) += 1
                break // TODO: check with Grazziela

              }
            }
            /*
          if (miles < MinDistance) {
            MinDistance = miles
            nearest = i
          }
          */
          }
        }
      }
      // If I have found a suppressor cell close by, it means this is redundant and can be eliminated.
      if(MinDistance != -1){
        cont += 1
        toBeRemoved += true
        // println("Need to be removed: "+MinDistance)
      }else{
        toBeRemoved += false
      }
      cont2 += 1

    } // End while loop for instances in this MAP.

    // Here I filter those spots that have been marked as redundant.

    println("I'm going to eliminate: "+cont + "; out of: "+cont2)

    val filteredSet = iterator2.zipWithIndex.filterNot{case (key, value) => toBeRemoved(value.toInt)}


    // the only way I see right now is to save this into separate files, and the aggregate them later in the main...

   /* println("fitness: ")
    for(i<-0 until fitnessSuppressor.size)
        print(fitnessSuppressor(i)+",")
*/

  /*  val allOutPutInfo = (filteredSet.map(line => line._1), fitnessSuppressor)

    allOutPutInfo.productIterator
*/
    filteredSet.map(line => line._1)
    //allOutPutInfo
  }
 */
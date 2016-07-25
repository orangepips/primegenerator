/*
Outputs values from both a single and multi threaded algorithm with benchmark timings. To stop either see the main method.
On an AMD 6200 using 3 cores this scales well to 5,000,000

scala -J-Xmx2g PrimeGenerator 1000000

Algorithm used is Sieve of Eratosthenes algorithm http://en.wikipedia.org/wiki/Sieve_of_Eratosthenes

Parallel algorithm approach - equal sized blocks - taken from:

 1. http://www.shodor.org/media/content//petascale/materials/UPModules/sieveOfEratosthenes/module_document_pdf.pdf
 1. http://www.massey.ac.nz/~mjjohnso/notes/59735/seminars/01077635.pdf

Goals of parallel algorithm are:

 1. avoid locks (e.g. semaphores)
 2. avoid redundant work between tasks (if possible)
 3. and balance amount of work between tasks

 */

import java.lang.Runnable
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.math._
import scala.util.control.Breaks._

object PrimeGenerator {
  def main(args: Array[String]) {
    val numberOfElements = (if (args.length > 0) args(0).toInt else 100000000) + 1

    time { singleThread(numberOfElements)}

    time { multiThread(numberOfElements) }
  }

  def multiThread(numberOfElements: Int): Unit = {
    println("MULTI THREADED")
    val isComposite = new Array[Boolean](numberOfElements)
    // "processes" could be manually set to "2" - micro benchmarking found this was a good compromise
    val processes = Math.max(1, Math.ceil(Runtime.getRuntime().availableProcessors() / 2)).toInt

    println("processes:\t" + processes)

    val pool = Executors.newFixedThreadPool(processes)
    for(i <- 0 until processes) {
      pool.submit(new Block(i, processes, isComposite))
    }
    pool.shutdown()
    pool.awaitTermination(60, TimeUnit.SECONDS);

    printPrimes(isComposite)

  }

  def singleThread(numberOfElements: Int): Unit = {
    println("SINGLE THREADED")
    val numberOfElementsSqrt = floor(sqrt(numberOfElements)).toInt
    val isComposite = new Array[Boolean](numberOfElements)

    var i = 2

    while (i < numberOfElementsSqrt) {
      //println("i:\t" + i)
      var p = Math.pow(i, 2)

      while (p < numberOfElements) {
        //println("p:\t" + p)
        isComposite(p.toInt) = true
        p = p + i
      }
      breakable { for (j <- i + 1 to numberOfElements) {
        //println("j:\t" + j)
        if (!isComposite(j)) {
          i = j
          break
        }
      } }
    }
    printPrimes(isComposite)
  }

  def printPrimes(isComposite: Array[Boolean]): Unit = {
    val primes = new ListBuffer[Int]
    for ( i <- 0 to (isComposite.length - 1)) {
      if (!isComposite(i) && i >= 2) primes.append(i)
    }

    val limit = 1000

    val primesString = (if (primes.length > limit) primes.takeRight(limit) else primes).mkString(", ")
    if (primes.length > 1000) {
      println("Only last " + limit + " primes shown")
    }
    println("Count: " + primes.length + " | Primes: " + primesString)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  class Block(processId: Int, processes: Int, isComposite: Array[Boolean]) extends Runnable {
    def run(): Unit = {
      val numberOfElements = isComposite.length
      val primesLtNumberOfElementsSqrt = getPrimesLtNumberOfElementsSqrt(isComposite)
      var low = blockLow(processId, processes, numberOfElements)
      if (low < primesLtNumberOfElementsSqrt.last) low = primesLtNumberOfElementsSqrt.last + 1
      val high = blockHigh(processId, processes, numberOfElements)

      //println(processId + ":\t" + low + " - " + high)

      for(prime <- primesLtNumberOfElementsSqrt) {
        var composite = prime + prime
        while (composite < low) {
          composite += prime
        }
        while (composite <= high) {
          isComposite(composite) = true
          composite += prime
        }
      }
    }
  }

  def getPrimesLtNumberOfElementsSqrt(isComposite: Array[Boolean]): ListBuffer[Int] = {
    val numberOfElementsSqrt = floor(sqrt(isComposite.length)).toInt
    val primesLtNumberOfElementsSqrt = new ListBuffer[Int]

    var prime = 2
    while (prime < numberOfElementsSqrt) {
      primesLtNumberOfElementsSqrt.append(prime)
      var p = Math.pow(prime, 2)
      while (p < numberOfElementsSqrt) {
        isComposite(p.toInt) = true
        p = p + prime
      }
      breakable {
        for (j <- prime + 1 to numberOfElementsSqrt) {
          if (!isComposite(j)) {
            prime = j
            break
          }
        }
      }
    }
    return primesLtNumberOfElementsSqrt
  }
  /*
  #define BLOCK_LOW(id,p,n) ((i)*(n)/(p))
  #define BLOCK_HIGH(id,p,n) (BLOCK_LOW((id)+1,p,n)-1)
  #define BLOCK_SIZE(id,p,n) (BLOCK_LOW((id)+1)-BLOCK_LOW(id))
  #define BLOCK_OWNER(index,p,n) (((p)*(index)+1)-1)/(n))
  */
  def blockLow(processId: Int, processes: Int, numberOfElements: Int): Int = {
    return processId * numberOfElements / processes
  }

  def blockHigh(processId: Int, processes: Int, numberOfElements: Int): Int = {
    return blockLow(processId + 1, processes, numberOfElements) - 1
  }

  def blockSize(processId: Int, processes: Int, numberOfElements: Int): Int = {
    return blockLow(processId + 1, processes, numberOfElements) - blockLow(processId, processes, numberOfElements)
  }

  def blockOwner(index: Int, processes: Int, numberOfElements: Int): Int = {
    return ((((processes)*(index)+1)-1)/(numberOfElements))
  }
}
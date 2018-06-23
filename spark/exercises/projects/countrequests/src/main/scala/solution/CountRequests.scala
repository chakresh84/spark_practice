package solution

import org.apache.spark.SparkContext

object CountRequests {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: solution.CountRequests <logfile>")
       System.exit(1)
     }

     val sc = new SparkContext()

     val logfile = args(0)
     
     sc.setLogLevel("WARN")
     
     val csrcount = sc.textFile(logfile).
        filter(line => line.contains("CSR")).
        count()

     println( "Number of CSR requests: " + csrcount)
     
     val mobilecount = sc.textFile(logfile).
        filter(line => line.contains("Mobile")).
        count()

     println( "Number of Mobile requests: " + mobilecount)

     sc.stop
   }
 }


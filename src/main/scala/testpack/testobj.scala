package testpack


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import scala.io.Source


object testobj {
  
  def main(args:Array[String]):Unit={




		val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)   // RDD

		sc.setLogLevel("ERROR")

		val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

		import spark.implicits._

	 // val filedata = spark.read.format("parquet").load("file:///home/cloudera/data/project.parquet")

		
		//filedata.show()
		val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=3").mkString
		println(urldata)
			
		val df =  spark.read.json(sc.parallelize(List(urldata)))
		df.printSchema()

		val explodedf = df.withColumn("results",expr("explode(results)"))
		explodedf.printSchema()
		val flattendf = explodedf.select(
		                           "nationality",
		                           "results.user.cell",
		                           "results.user.dob",
		                           "results.user.email",
		                           "results.user.gender",
		                           "results.user.location.city",
		                           "results.user.location.state",
		                           "results.user.location.street",
		                           "results.user.location.zip",
		                           "results.user.md5",
		                           "results.user.name.first",
		                           "results.user.name.last",
		                           "results.user.name.title",
		                           "results.user.password",
		                           "results.user.phone",
		                           "results.user.picture.large",
		                           "results.user.picture.medium",
		                           "results.user.picture.thumbnail",
		                           "results.user.registered",
		                           "results.user.salt",
		                           "results.user.sha1",
		                           "results.user.sha256",
		                           "results.user.username",
		                           "seed",
		                           "version"
		                          )
		                          
    
    
    val final_flat = flattendf.withColumn("username",regexp_replace(col("username"),"([0-9])",""))
   final_flat.show() 
  //  val joindf = filedata.join(broadcast(flattendf),Seq("username"),"left")
    
 //   joindf.show()
		
//    joindf.write.format("parquet").mode("overwrite").save("file:///home/cloudera/write_data")
//
//    println("===>data written to the target")

  }
}




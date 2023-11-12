
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.state.StateStore.stop
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

import scala.::

object dfpractice extends App{
val spark=SparkSession.builder().appName("my spark df program").master("local[1]").getOrCreate()
  val rdd: RDD[Row] = spark.sparkContext.parallelize(Seq(Row("Abhi", 25, 97321.25), Row("Avi", 27, 215327.23), Row("Srinivasu", 38, 28715.0)))
 // rdd.foreach(println)

  //val schema = StructType(
 //   StructField("Name",StringType,false)::
 //   StructField("Age",IntegerType,true)::
 //   StructField("Salary",DoubleType,true)::Nil)

 // val df =spark.createDataFrame(rdd,schema)
 // df.show()
//val rdd1=spark.sparkContext.textFile("C:\\putty\\iris.json")
  //val schem=StructType(Seq(StructField("sepalLength",FloatType,true),
   // StructField("sepalWidth",FloatType,true),StructField("petalLength",FloatType,true),StructField("petalWidth",FloatType,true)))
  import spark.implicits._
//val rowrdd=rdd1.map{line=>val data=line.split(",")
 // Row(data(0).toDouble,data(1).toDouble,data(2).toDouble,data(3).toDouble)
//}
  //val dfj=spark.createDataFrame(rowrdd,schem)
 // dfj.take(100).foreach(println)
// reading csv file and creating data frame
  val dfcsv=spark.read.option("header",true).option("inferschema",true).csv("C:\\putty\\datanew.csv")
  dfcsv.show()
//reading json file and creating data frame
  val dfjson = spark.read.option("multiline",true).json("C:\\putty\\iris.json")
  dfjson.show()

  dfcsv.createOrReplaceTempView("csv_table")
  val required=spark.sql("select Product,sum(Amount) as sop from csv_table Group By Product Order by sop ")
  required.show()

dfjson.createOrReplaceTempView("iris_table")
  val ireq =spark.sql("select Avg(petalLength),avg(petalWidth),Avg(sepalLength),Avg(sepalWidth),species from iris_table Group by species ")
  ireq.show()
dfjson.select("PetalLength","PetalWidth","sepalLength","sepalWidth","species").groupBy("species").avg("PetalLength","PetalWidth","sepalLength","sepalWidth").show()
dfcsv.printSchema()

  dfcsv.select("Country","Product","Amount","Quantity").groupBy("Country","Product").sum("Amount","Quantity").orderBy("Country").show()
stop()
  dfcsv.createOrReplaceTempView("ranks")
 // val rankt=spark.sql("select Name,Country,sum(Amount) total ,dense_rank() over (Order by sum(Amount)  Desc) rank from ranks Group by Name,Country")
  //rankt.show()
  //rankt.write.format("avro").mode("overwrite").save("C:\\putty\\abhi\\")
val dfp=spark.read.format("avro").load("C:\\putty\\abhi\\part-00000-6206932b-02c2-4b45-b693-f5915b45b716-c000.avro")
  dfp.show()
}

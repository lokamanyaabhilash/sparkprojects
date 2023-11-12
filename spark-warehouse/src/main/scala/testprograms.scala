import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, row_number, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.row_number
object testprograms extends App {
  val spark =SparkSession.builder().appName("my practice test").master("local[1]").getOrCreate()


  // 1
val list1=List("Lion","Tiger","Elephant")
  val list2=List("King","Fast Runner","Heavy Weight")
 val x:String=" is "
  val result=List(list1(0)+x+list2(0),list1(1)+x+list2(1),list1(2)+x+list2(2))
println(result)

  //2
  case class Employee(EmployeeID:String,FirstName:String,DepartmentID:Int,Gender:String,Age:Int,Salary:Int,country:String)
  case class Department(DepartmentID:Int,DepartmentName:String)
val schemaee=Encoders.product[Employee].schema
val schemad=Encoders.product[Department].schema
val dfe=spark.read.schema(schemaee).csv("C:\\putty\\employee.csv")
  dfe.show()
  val dfd=spark.read.schema(schemad).csv("C:\\putty\\department.csv")
  dfd.show()

  dfe.createOrReplaceTempView("employee")
  dfd.createOrReplaceTempView("department")

  val sol=spark.sql("""select EmployeeID,FirstName,DepartmentName,max_salary,salary from
                     (select e.EmployeeID,e.FirstName,d.DepartmentName,max(e.Salary) over(partition by d.DepartmentName) max_salary,
                      e.Salary, Row_Number() over(partition by d.DepartmentName order by e.Salary Desc ) row from employee e
                      join department d on e.DepartmentID=d.DepartmentID)where row=5 """)
  sol.show()
  val windowSpec = Window.partitionBy("d.DepartmentName").orderBy(col("e.Salary").desc)

val sol1=dfe.alias("e").join(dfd.alias("d"),col("d.DepartmentID")===col("e.DepartmentID"),"inner").
  withColumn("max_salary", max(col("e.salary")).over(windowSpec))
  .withColumn("Row",row_number().over(windowSpec)).filter("Row=5")
  .select("e.EmployeeID","e.FirstName","d.DepartmentName","max_Salary","e.salary","Row")

sol1.show()

  //3
  val df2=spark.read.option("header",true).csv("C:\\putty\\active.csv")
  df2.show()
  df2.createOrReplaceTempView("office")
val result3=spark.sql(
  """select id,name,dept,salary_month,salary,row from
    (select id,name,dept,salary_month,salary,Row_Number() over(partition by dept,salary_month Order by salary Desc) row
    from office) where row <=3 order by salary_month,dept""")
  result3.show()

  //4
  import spark.implicits._

  val rdd = spark.sparkContext.parallelize(Seq(
    "1,2020-11-28,4,32",
    "1,2020-11-28,55,200",
    "1,2020-12-03,1,42",
    "2,2020-11-28,3,33",
    "2,2020-12-09,47,74"
  ))

  val rdd1 = rdd.map(line => {
    val fields = line.split(",")
    Row(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  })

  val schema = StructType(Seq(
    StructField("emp_id", IntegerType, true),
    StructField("event_day", StringType, true),
    StructField("in_time", IntegerType, true),
    StructField("out_time", IntegerType, true)
  ))

  val df3 = spark.createDataFrame(rdd1, schema)
  df3.show()
df3.createOrReplaceTempView("events")
  val result4=spark.sql("select event_day,emp_id,sum(out_time-in_time) total_time from events group by event_day,emp_id ")
  result4.show()
  //val windowSpec = Window.partitionBy("emp_id").orderBy("event_day")

  val result5= df3
    .withColumn("total_time", col("out_time").minus(col("in_time")))
    .select("event_day", "emp_id", "total_time").groupBy("emp_id","event_day").sum("t
result5.show()
\\lokamanya Abhilash Pragada
}

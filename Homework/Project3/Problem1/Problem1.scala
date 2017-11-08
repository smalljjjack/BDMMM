import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.SparkContext

val spark = SparkSession.builder().appName("Problem1").config("spark.master","local").getOrCreate()

val Transactions = spark.read.format("csv").option("header", "true").load("/home/mqp/Documents/Project3/Dataset/Transactions")

val Customer = spark.read.format("csv").option("header", "true").load("/home/mqp/Documents/Project3/Dataset/Customers")

val result1 = Transactions.filter($"TransTotal" >= 200)
//T1
result1.show()

Transactions.createOrReplaceTempView("T")

result1.createOrReplaceTempView("T1")

val result2 = spark.sql("SELECT TransNumItems, SUM(TransTotal) AS SUM_TOTAL, AVG(TransTotal) AS AVG_TOTAL, MIN(TransTotal) AS MIN_TOTAL, MAX(TransTotal) AS MAX_TOTAL FROM T1 GROUP BY(TransNumItems)")
//T2
result2.show()

result2.write.format("csv").save("/home/mqp/Documents/Project3/Output/P1.T2_output")

val result3 = spark.sql("SELECT CustID, COUNT(*) AS T3_NUM_TRANS FROM T1 GROUP BY (CustID)")
//T3
result3.show()

val result4 = Transactions.filter($"TransTotal" >= 600)
//T4
result4.show()

result4.createOrReplaceTempView("T4")

val result5 = spark.sql("SELECT CustID, COUNT(*) AS T5_NUM_TRANS FROM T4 GROUP BY CustID")
//T5
val result6 = result5.join(result3,"CustID")

result6.createOrReplaceTempView("T5")

val finalResult = spark.sql("SELECT CustID, T5_NUM_TRANS, T3_NUM_TRANS FROM T5 WHERE T5_NUM_TRANS * 3 < T3_NUM_TRANS")
//T6
finalResult.show()

finalResult.write.format("csv").save("/home/mqp/Documents/Project3/Output/P1.T6_output")
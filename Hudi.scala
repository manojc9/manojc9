import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.SaveMode

val data = Seq(
  ("1", "John", "Doe"),
  ("2", "Jane", "Smith")
)

val df = spark.createDataFrame(data).toDF("id", "first_name", "last_name")

df.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "id")
  .option("hoodie.table.name", "test_hudi_table")
  .mode(SaveMode.Overwrite)
  .save("/tmp/hudi_test")

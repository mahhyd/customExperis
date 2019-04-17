import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object runProcess extends App {
  val spark = SparkSession
    .builder
    .appName("appCustomerExperis")
    .master("local")
    .getOrCreate()

  //************************* First Part (load cvs, text and Json file on DataFrame **********************************
  //load tornik-map.txt
  val dfTornikMap = spark.read.textFile("src/main/ressource/tornik-map.txt")
  //dfTornikMap.show(10)

  dfTornikMap.createOrReplaceGlobalTempView("tornikMap")
  spark.sql("SELECT * FROM global_temp.tornikMap").show(20)

  //load customer.csv with inferSchema
  val dfCustomer = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/customer.csv")
  //dfCustomer.show(5)

  //load region.csv with schema
  val regionSchema = StructType(Array(
    StructField("region_id", IntegerType, true),
    StructField("sales_city", StringType, true),
    StructField("sales_state_province", StringType, true),
    StructField("sales_district", StringType, true),
    StructField("sales_region", StringType, true),
    StructField("sales_country", StringType, true),
    StructField("sales_district_id", IntegerType, true)))

  val dfRegion = spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ";")
    .schema(regionSchema)
    .load("src/main/ressource/region.csv")
  //dfRegion.show(10)


  //load product.csv with inferSchema
  val dfProduct = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/product.csv")

  //dfProduct.show(10)
  //dfProduct.printSchema()

  //load product_class.csv with inferSchema
  val dfProduct_class = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/product_class.csv")
  //dfProduct_class.printSchema()

  //load product_class.csv with inferSchema
  val dfPromotion = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/promotion.csv")
  //dfPromotion.printSchema()

  //load sales.csv with inferSchema
  val dfSales = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/sales.csv")
  //dfSales.printSchema()

  //load store.csv with inferSchema
  val dfStore = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/store.csv")
  //dfStore.printSchema()

  //load store.csv with inferSchema
  val dfTime = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/ressource/time.csv")
  //dfTime.printSchema()

  //************************* Second  Part (manipulate Operations DataFrame) **********************************
  //print schema Product DataFrame
  dfProduct.printSchema()

  //select only the "name" column
  dfProduct.select("product_name").show()

  //select name and net_weight, but increment the net_weight by 3
  dfProduct.select(dfProduct.col("product_id"), dfProduct.col("net_weight") + 3).show(15)

  //filter
  dfProduct.filter(dfProduct.col("product_id") < 10).show()

  // Count product by product_class_id
  dfProduct.groupBy(dfProduct.col("product_id")).count().show(19)

  //Register the DataFrame as a SQL temporary view
  dfProduct.createOrReplaceTempView("viewProduct")
  spark.sql("select product_name, product_class_id" +
    " from viewProduct " +
    "where brand_name = \"Washington\" " +
    "and product_class_id > 29").show(12)

  //Register the DataFrame as a SQL global temporary view
  dfProduct.createOrReplaceGlobalTempView("globalViewProduct")
  //spark.sql("select * from globalViewProduct").show(14)
  //spark.newSession().sql("select * from globalViewProduct").show(16)

  //


  //************************* third Part (use case class for convert DF to DS) *******************************
}
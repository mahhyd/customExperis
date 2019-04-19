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


  //************************* third Part (use case class for convert DF to DS) *******************************
  //load region.csv with schema
  val regionSchemattt = StructType(Array(
    StructField("region_id", IntegerType, true),
    StructField("sales_city", StringType, true),
    StructField("sales_state_province", StringType, true),
    StructField("sales_district", StringType, true),
    StructField("sales_region", StringType, true),
    StructField("sales_country", StringType, true),
    StructField("sales_district_id", IntegerType, true)))
  // 1.Create DataSet from StructType
  import spark.implicits._
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Region(region_id: Int, sales_city: String, sales_state_province: String, sales_district: String, sales_region: String, sales_country: String, sales_district_id: Int)
  // Encoders are created for case classes
  val caseClassRegionDS1 = Seq(Region(1, "San Francisco", "CA", "San Francisco", "Central West", "USA", 123)).toDS()
  caseClassRegionDS1.show()

  // 2.Create DataSet from file using case class
  val caseClassRegionDS2 = dfRegion.as[Region]
  caseClassRegionDS2.show(16)

  // 3.Create DataSet from text file structured
  val dfRegiontxt = spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ";")
    .schema(regionSchema)
    .load("src/main/ressource/region.txt")
  dfRegiontxt.show(10)

  // 3.Create DataSet from text file not structured using case class
  val dfRegiontxt1 = spark.read.textFile("src/main/ressource/region.txt").map(x => x.split(";")).show(11)

  val dfRegiontxt2 = spark.read.textFile("src/main/ressource/region.txt").map(x => x.split(";")).map(arrays => Region(arrays(0).toInt, arrays(1).toString, arrays(2).toString, arrays(3).toString, arrays(4).toString, arrays(5).toString, arrays(6).toInt))
  dfRegiontxt2.show(9)

  // 4.Create DataSet from rdd
  val rddRegion = spark.sparkContext.textFile("src/main/ressource/region.txt")
  //rddRegion.collect().map(line => println(line))

  // 4.1.Create DataSet from rdd using case class
  val dfRegion1 = rddRegion.map(_.split(";")).map(arrays => Region(arrays(0).toInt, arrays(1).toString, arrays(2).toString, arrays(3).toString, arrays(4).toString, arrays(5).toString, arrays(6).toInt)).toDS()
  dfRegion1.show(17)

  // 4.2. Create DataSet from rdd using StructureType



}

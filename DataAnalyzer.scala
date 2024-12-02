package SalesAnalysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}
import java.io.ByteArrayOutputStream
import java.io.PrintStream

object DataAnalyzer {
  val spark: SparkSession = SparkSession.builder()
    .appName("Sales Analysis")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()

  import spark.implicits._

  def loadDataset(path: String): Try[DataFrame] = {
    Try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .csv(path)
    }
  }

  def captureShowString(df: DataFrame, numRows: Int = 20): String = {
    val outCapture = new ByteArrayOutputStream()
    val printStream = new PrintStream(outCapture)
    Console.withOut(printStream) {
      df.show(numRows, truncate = false)
    }
    outCapture.toString("UTF-8")
  }

  def saveAsCsv(df: DataFrame, path: String): Unit = {
    df.write
      .option("header", "true")
      .mode("overwrite")
      .csv(path)
  }


  def analyzeData(customersPath: String, itemsPath: String, ordersPath: String, productsPath: String): String = {
    val results = new StringBuilder()

    val customersTry = loadDataset(customersPath)
    val itemsTry = loadDataset(itemsPath)
    val ordersTry = loadDataset(ordersPath)
    val productsTry = loadDataset(productsPath)

    (customersTry, itemsTry, ordersTry, productsTry) match {
      case (Success(customers), Success(items), Success(orders), Success(products)) =>
        val customersDS = customers.as[Customer]
        val itemsDS = items.as[Item]
        val ordersDS = orders.as[Order]
        val productsDS = products.as[Product]

        // Analyse 1 : Ventes par client
        val salesByCustomer = ordersDS
          .join(itemsDS, "order_id")
          .join(customersDS, "customer_id")
          .groupBy("customer_unique_id")
          .agg(
            countDistinct("order_id").as("total_orders"),
            sum("price").as("total_spent")
          )
          .orderBy(desc("total_spent"))

        results.append("=== Ventes par client ===\n")
        results.append(captureShowString(salesByCustomer, 10))
        saveAsCsv(salesByCustomer, "C:\\Users\\aziza\\IdeaProjects\\SalesAnalysisGUI\\src\\main\\scala\\sales_by_customer.csv")

        // Analyse 2 : Clients récurrents
        val recurringCustomers = salesByCustomer.filter($"total_orders" > 1)
        val recurringCount = recurringCustomers.count()
        results.append(s"\nNombre de clients récurrents : $recurringCount\n")

        // Analyse 3 : Panier moyen par catégorie
        val avgBasketByCategory = itemsDS
          .join(productsDS, "product_id")
          .groupBy("product_category_name_english")
          .agg(
            (sum("price") / countDistinct("order_id")).as("average_basket")
          )
          .orderBy(desc("average_basket"))

        results.append("\n=== Panier moyen par catégorie ===\n")
        results.append(captureShowString(avgBasketByCategory, 10))
        saveAsCsv(avgBasketByCategory, "C:\\Users\\aziza\\IdeaProjects\\SalesAnalysisGUI\\src\\main\\scala\\avg_basket_by_category.csv")

        // Analyse 4 : Produits les plus populaires
        val mostPopularProducts = itemsDS
          .groupBy("product_id")
          .agg(count("order_id").as("total_sold"))
          .join(productsDS, "product_id")
          .orderBy(desc("total_sold"))

        results.append("\n=== Produits les plus populaires ===\n")
        results.append(captureShowString(mostPopularProducts, 10))
        saveAsCsv(mostPopularProducts, "C:\\Users\\aziza\\IdeaProjects\\SalesAnalysisGUI\\src\\main\\scala\\most_popular_products.csv")

        // Analyse 5 : Segmentation des clients
        val customerSegmentation = salesByCustomer.withColumn(
          "segment",
          when($"total_spent" > 5000, "High Spender")
            .when($"total_spent" > 1000, "Medium Spender")
            .otherwise("Low Spender")
        ).withColumn(
          "frequency_segment",
          when($"total_orders" > 5, "Fréquant")
            .when($"total_orders" > 2, "Occasionnel")
            .otherwise("Rare")
        )

        results.append("\n=== Segmentation des clients ===\n")
        results.append(captureShowString(customerSegmentation, 10))
        saveAsCsv(customerSegmentation, "C:\\Users\\aziza\\IdeaProjects\\SalesAnalysisGUI\\src\\main\\scala\\customer_segmentation.csv")

      case _ =>
        results.append("Erreur : Échec du chargement des fichiers. Veuillez vérifier les chemins.")
    }

    results.toString()
  }
}



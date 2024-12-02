package SalesAnalysis

case class Item(
                 order_id: String,
                 order_item_id: Int,
                 product_id: String,
                 seller_id: String,
                 shipping_limit_date: String,
                 price: Double,
                 freight_value: Double
               )

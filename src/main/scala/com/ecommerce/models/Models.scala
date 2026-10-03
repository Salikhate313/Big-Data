package com.ecommerce.models

/** Case classes des quatre jeux de données (Membre A).
  * Les champs numériques sont des Option pour tolérer les valeurs manquantes
  * volontairement présentes dans les fichiers sources. */

case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String,          // yyyyMMddHHmmss
  location: String,
  payment_method: String,
  category: String
)

case class User(
  user_id: String,
  age: Option[Int],
  annual_income: Option[Double],
  city: String,
  customer_segment: String,
  preferred_categories: Seq[String],
  registration_date: String   // yyyyMMdd
)

case class Product(
  product_id: String,
  name: String,
  category: String,
  price: Option[Double],
  merchant_id: String,
  rating: Option[Double],
  stock: Option[Int]
)

case class Merchant(
  merchant_id: String,
  name: String,
  category: String,
  region: String,
  commission_rate: Option[Double],
  establishment_date: String  // yyyyMMdd
)

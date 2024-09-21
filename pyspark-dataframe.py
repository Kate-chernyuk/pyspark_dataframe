from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_product_category_pairs(products_df, product_categories_df):
    product_category_pairs = product_categories_df.select("product_name", "category_name")
    
    products_with_categories = product_categories_df.select("product_name").distinct()
    products_without_categories = products_df.join(products_with_categories, "product_name", "left_anti")

    return product_category_pairs, products_without_categories


spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

products_data = [("Product A",), ("Product B",), ("Product C",)]
categories_data = [("Category 1",), ("Category 2",)]
product_categories_data = [
    ("Product A", "Category 1"),
    ("Product A", "Category 2"),
    ("Product B", "Category 1"),
]

products_df = spark.createDataFrame(products_data, ["product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_name"])
product_categories_df = spark.createDataFrame(product_categories_data, ["product_name", "category_name"])

product_category_pairs, products_without_categories = get_product_category_pairs(products_df, product_categories_df)

print("Пары 'Имя продукта – Имя категории':")
product_category_pairs.show()

print("Продукты без категорий:")
products_without_categories.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit

### 1. Initialize SparkSession with custom master URL
spark = SparkSession.builder \
    .appName("NormalizeData") \
    .master("spark://spark-master.storage.idc.coe.hv:7077") \
    .getOrCreate()

### 2. Read the TSV file from HDFS ###
df = spark.read.option("delimiter", "\t").option("header", True).option("inferSchema", True).csv("hdfs://spark-master.storage.idc.coe.hv:9000/county_market_tracker.tsv")
#df = spark.read.option("delimiter", "\t").option("header", True).option("inferSchema", True).csv("hdfs://spark-master.storage.idc.coe.hv:9000/county_market_tracker_small.tsv")
#df = spark.read.option("delimiter", "\t").option("header", True).option("inferSchema", True).csv("hdfs://spark-master.storage.idc.coe.hv:9000/city_market_tracker.tsv")

### 3. Process initial data ###
# 3.1 Get rid of unused columns
getRidOfColumns = ["period_duration", "region_type", "region_type_id", "table_id", "is_seasonally_adjusted", "city", "state_code", "property_type_id", "median_list_price", "median_list_price_mom", "median_list_price_yoy", "median_ppsf", "median_ppsf_mom", "median_ppsf_yoy", "median_list_ppsf", "median_list_ppsf_mom", "median_list_ppsf_yoy", "pending_sales", "pending_sales_mom", "pending_sales_yoy", "new_listings", "new_listings_mom", "new_listings_yoy", "inventory", "inventory_mom", "inventory_yoy", "months_of_supply", "months_of_supply_mom", "months_of_supply_yoy", "median_dom", "median_dom_mom", "median_dom_yoy", "avg_sale_to_list", "avg_sale_to_list_mom", "avg_sale_to_list_yoy", "sold_above_list", "sold_above_list_mom", "sold_above_list_yoy", "price_drops", "price_drops_mom", "price_drops_yoy", "off_market_in_two_weeks", "off_market_in_two_weeks_mom", "off_market_in_two_weeks_yoy", "parent_metro_region_metro_code", "last_updated"]
df = df.drop(*getRidOfColumns)
df.count()

# 3.2 Filter out entries that are not of property "All Residential" and where column records for homes_sold_mom and homes_sold_yoy are null
filtered_df = df.filter((df['property_type'] == 'All Residential') & (df.homes_sold_mom.isNotNull()) & (df.homes_sold_yoy.isNotNull()))
filtered_df.count()
filtered_df.show(5)

# 3.3 Sort data by region and then by period_begin date, just becasue it is eaiser to see the data if we want to look at it
filtered_df = filtered_df.sort(df.region, df.period_begin)

# 3.4 Group by the region name and calculate average homes_sold_mom as well as average homes_sold_yoy
homes_sold_avg = filtered_df.groupby('region').agg({'homes_sold_mom':'avg', 'homes_sold_yoy':'avg'})

# 3.5 Add average homes_sold_mom and homes_sold_yoy together and divide by 2 to get a total score for time over time amount
homes_sold_avg = homes_sold_avg.withColumn('avgTotal', ((col('avg(homes_sold_mom)') + col('avg(homes_sold_yoy)' )) / lit(2)))

### 4. Scale avgTotal to the range 0 - 1 and verify ###
# 4.1 Find the min and max values for scaling the avgTotal amounts
min_max_values = homes_sold_avg.select(min("avgTotal"), max("avgTotal")).collect()[0]
min_value, max_value = min_max_values["min(avgTotal)"], min_max_values["max(avgTotal)"]

# 4.2 Scale avgTotal to the range 0 - 1 and verify
# using the equation: y = (x - old_min) * ( (new_max - new_min) / (old_max - old_min) ) + new_min
homes_sold_avg = homes_sold_avg.withColumn('avgTotalScaled', lit((col('avgTotal') - min_value) * ( ( 1 - 0 ) / (max_value - min_value) ) + 0))

# 4.3 Validate that the min of the new column is 0 and max is 1
# test_min_max_values = homes_sold_avg.select(min("avgTotalScaled"), max("avgTotalScaled")).collect()[0]
# test_min_value, test_max_value = test_min_max_values["min(avgTotalScaled)"], test_min_max_values["max(avgTotalScaled)"]
# print(test_min_value)
# print(test_max_value)

### 5. Save the data back to HDFS ###
# 5.1 Save the scaled data back to HDFS
homes_sold_avg.write.option("delimiter", ",").mode("overwrite").csv("hdfs://spark-master.storage.idc.coe.hv:9000/scaled_county_market_tracker.csv")

# 5.2 Save the altered filtered dataframe from above, if you want to
# filtered_df.write.option("delimiter", ",").mode("overwrite").csv("hdfs://spark-master.storage.idc.coe.hv:9000/normalized_county_market_tracker.csv")

### 6. Stop SparkSession ###
spark.stop()

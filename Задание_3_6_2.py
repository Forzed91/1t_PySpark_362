from pyspark.sql import SparkSession
from datetime import date
import pandas as pd
import random
#from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()
#
start_date = date.today().replace(day=1, month=1).toordinal()
end_date = date.today().toordinal()
random_day = date.fromordinal(random.randint(start_date, end_date))
product_list = ['Молоко', 'Хлеб','Консервы','Яблоки','Кефир','Колбаса','Сыр','Конфеты']
data = {'date':[],'userID':[], 'product':[], 'count': [], 'price': []}
count = 1000 # количество синтезируемых записей
for i in range(0, count):
  data['date'].append(date.fromordinal(random.randint(start_date, end_date)))
  data['product'].append(product_list[random.randint(0, len(product_list)-1)])
  data['userID'].append(random.randint(0, 1000))
  data['count'].append(random.randint(1, 100))
  data['price'].append(random.randint(100, 10000))
#
df = pd.DataFrame(data)
columns = ['date', 'userID', 'product', 'count', 'price']
dfOrder = spark.createDataFrame(df, schema=columns)
dfOrder.show()
dfOrder.write.csv('mycsvOrder.csv')
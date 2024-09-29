# parquet-varient
parquet-varient
![Screenshot 2024-09-29 at 8 28 02 AM](https://github.com/user-attachments/assets/003d52c7-4bf6-4704-b520-c4c4867e5c30)


### INstall 
```
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"

pip install 4.0.0.dev2
```
### Writer 
```
# write.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import parse_json

# Initialize Spark session with Spark 4.0
spark = SparkSession.builder \
    .appName("VariantTypeSupportApp") \
    .getOrCreate()

# Create a DataFrame with a complex JSON string
df1 = spark.createDataFrame([{
    'json_string': '''{
        "user": {
            "name": "Alice",
            "age": 30,
            "hobbies": ["reading", "swimming", "hiking"],
            "address": {
                "city": "Wonderland",
                "zip": "12345"
            }
        },
        "status": "active",
        "tags": ["admin", "user", "editor"]
    }'''
}])

# Parse the JSON string to a VariantType column
df2 = df1.select(
    parse_json(df1.json_string).alias("json_var")
)

# Write the DataFrame with VariantType to Parquet
df2.write.mode("append").parquet("variant_data.parquet")

print("Data written to Parquet file: variant_data.parquet")

```


### Reader
```

# read.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import try_variant_get

# Initialize Spark session with Spark 4.0
spark = SparkSession.builder \
    .appName("ReadVariantTypeFromParquet") \
    .getOrCreate()

# Read the Parquet file containing the VariantType column
df_read = spark.read.parquet("variant_data.parquet")

# Access specific fields from the parsed JSON after reading from Parquet
df3 = df_read.select(
    try_variant_get("json_var", "$.user.name", "STRING").alias("name"),
    try_variant_get("json_var", "$.user.age", "INT").alias("age"),
    try_variant_get("json_var", "$.user.hobbies[0]", "STRING").alias("first_hobby"),
    try_variant_get("json_var", "$.status", "STRING").alias("status"),
    try_variant_get("json_var", "$.tags[1]", "STRING").alias("second_tag")
)

# Show the results after reading from Parquet
print("Results after reading from Parquet:")
df3.show()
df3.printSchema()

```

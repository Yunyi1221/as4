from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main(input_path, output_path):
    # Create a Spark session
    spark = (SparkSession
             .builder
             .appName("Word Count Program")
             .getOrCreate())

    # Read the input file
    text_df = spark.read.text(input_path)

    # Split lines into words
    words_df = text_df.select(explode(split(col("value"), " ")).alias("word"))

    # Remove empty strings resulting from splitting
    words_df = words_df.filter(col("word") != "")

    # Count occurrences of each word
    word_counts_df = words_df.groupBy("word").count()

    # Write the results to HDFS in CSV format
    word_counts_df.write.csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: word_count <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)

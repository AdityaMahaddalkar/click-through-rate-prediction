import pyspark


def main():
    # Create a SparkContext
    sc = pyspark.SparkContext()

    # Load some data into a Spark RDD
    data = sc.textFile("data/input.txt")

    # Split the data into words
    words = data.flatMap(lambda line: line.split(" "))

    # Count the number of occurrences of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # Get the top 10 most frequent words
    top10 = wordCounts.takeOrdered(10, key=lambda x: -x[1])

    print(top10)


if __name__ == "__main__":
    print('Started Python container')

    main()

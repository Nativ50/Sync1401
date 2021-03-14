import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{LongType, StringType, StructType}

// The schema of our data:
val schema = new StructType().
     add("ID","INTEGER",false).
     add("PLACE","STRING",false).
     add("TEXT","STRING",false)

// Reading the data from a CSV in a SparkSQL table:
val df = spark.
	read.
	schema(schema).
	csv("/Users/unix_slamd64/Desktop/Bin_Test/Tweet/tweets.csv")
	df.createOrReplaceTempView("TWEETS")
// Filtering out tweets from America:
spark.
	table("TWEETS").
	select("PLACE","TEXT").
	where("PLACE != ' America'").
	groupBy("PLACE").count.
	limit(10).
	show

spark.sql("""
	SELECT PLACE, COUNT(*) AS count
	FROM TWEETS
	WHERE PLACE != ' America'
	GROUP BY PLACE
	LIMIT 10
""").show
val words = df.
	select(
		explode(
			split($"text"," ")
		).alias("word")
	).where("word != ''").
	groupBy("word").count.
	show

spark.sql("""
  |SELECT word, count(*)
  | FROM(SELECT explode(split(text, ' ')) as word From TWEETS)
  | WHERE word !=''
  | GROUP BY word
  | """).show
//word-count(hybrid)

spark.sql("SELECT explode(split(text, ' ')) as word FROM TWEETS").
	where("word != ''").groupBy("word").count.
	show

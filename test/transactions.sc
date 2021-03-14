import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Crating the data schema:
val strutturaTabella = new StructType().
  add("timestamp", LongType, false).
  add("company", StringType, false).
  add("amount", LongType, false)

// Loading the data:
val df = spark.
  read.
  schema(strutturaTabella).
  csv("/mnt/condivisa/transactions_1000.csv").
  select(
    to_date(from_unixtime(expr("timestamp/1000"))).
      alias("transactionDate"),
    $"company", $"amount"
  )
df.createOrReplaceTempView("TRANSACTIONS")

///////////////////////////////////////////////////
// Resolving (part of) the transactions exercise //
///////////////////////////////////////////////////

// Tot amount per company:
df.groupBy("company").sum("amount").show

// Average amount per company:
df.groupBy("company").avg("amount").show

// Grouped by days:
df.
  groupBy("company", "transactionDate").
    sum("amount").
  orderBy("company","transactionDate").
  show


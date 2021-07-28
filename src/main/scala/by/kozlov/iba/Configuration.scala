package by.kozlov.iba

import org.apache.spark.sql.SparkSession

object Configuration {
  val ss = SparkSession
    .builder()
    .appName("ibaTask")
    .master("local[*]")
    .config("spark.hadoop.fs.stocator.scheme.list", "cos")
    .config("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    .config("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    .config("fs.stocator.cos.scheme", "cos")
    .config("fs.cos.sparkobject123.access.key", sys.env("access.key"))
    .config("fs.cos.sparkobject123.endpoint", sys.env("endpoint"))
    .config("fs.cos.sparkobject123.secret.key", sys.env("secret.key"))
    .config("fs.cos.service.v2.signer.type", "false")
    .getOrCreate()
  val inputDB = ss.read.format("jdbc")
    .option("username", sys.env("dbUsername"))
    .option("password", sys.env("dbPassword"))
    .option("driver", "com.ibm.db2.jcc.DB2Driver")
    .option("partitionColumn","year")
    .option("lowerBound","2015")
    .option("upperBound","2018")
    .option("numPartitions","20")
    .option("url", "jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50000/BLUDB:user=" + sys.env("dbUsername") + ";PWD=" + sys.env("dbPassword") + ";Security=SSL;")
    .option("dbtable", "PRT00338.product_record")
    .load()
}

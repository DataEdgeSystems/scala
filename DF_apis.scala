package com.ebay.feeds.delete.daily_delete.google_delete


import org.apache.log4j.{Level, Logger}
import com.ebay.feeds.FeedsJob
import com.ebay.feeds.table.schema.SourceTables._
import org.apache.spark.sql.Column


/**
  * Created by enxie on 2017/10/25.
  */

object GGDailyDeleteJob extends App{
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

  }
}
class GGDailyDeleteJob (params: com.ebay.feeds.delete.daily_delete.google_delete.Parameter, formatStartDate: String)extends FeedsJob("GGDailyDeleteJob", mode = params.mode) {

  override  def run(): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    // step1 load two tables
    var gg_us_live_item = readFilesAsDF(inputPath = params.table_gg_us_live_item_path
                            ,inputFormat = params.table_dw_lstg_item_file_format,
                              delimiter = params.table_dw_lstg_item_delimiter,
                              schema = test_gg_table.dfSchema)
    var dw_lstg_item = readFilesAsDF(inputPath = params.table_dw_lstg_item_path,
                                      inputFormat = params.table_dw_lstg_item_file_format,
                                      delimiter = params.table_gg_us_live_item_delimiter,
                                      schema = test_dw_table.dfSchema)

//    val gg_us_live_item = spark.read.format(params.table_gg_us_live_item_file_format)
//      .option("header","true")
//      .option ("delimiter",params.table_gg_us_live_item_delimiter)
//      .load(params.table_gg_us_live_item_path)
//    var dw_lstg_item = spark.read.format(params.table_dw_lstg_item_file_format)
//      .option("header","true")
//      .option ("delimiter",params.table_dw_lstg_item_delimiter)
//      .load(params.table_dw_lstg_item_path)


    gg_us_live_item.show()

    //step2  dw filter us
    dw_lstg_item = dw_lstg_item.filter(dw_lstg_item("country") contains("us"))
    dw_lstg_item.show()

    import spark.implicits._
    val df_1 = gg_us_live_item.alias("gg").join(dw_lstg_item, gg_us_live_item("itemid") === dw_lstg_item("item_id"), "left")
      .filter(dw_lstg_item("item_id").isNull)
     // .select(gg_us_live_item.columns.map( gg_us_live_item( _ )):_*)
     // .select(gg_us_live_item.columns.map(v => gg_us_live_item(v)):_*)
      .select($"gg.*")
//        .select(gg_us_live_item.columns:_*)
    df_1.show()


    //TODO step5 output sth
  }
}

package com.ebay.feeds.delete.daily_delete.google_delete


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import com.ebay.feeds.FeedsJob
import com.ebay.feeds.MappingConst.{fbFeedIdSiteIdMap, siteIdNameMap}
import com.ebay.feeds.table.schema.SourceTables._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, udf}
import com.ebay.feeds.MappingConst._


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

    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.ERROR)
    val dwId = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date())

    //todo step1 Load four tables
    val gg_us_live_item             = readFilesAsDF(inputPath = params.table_gg_us_live_item_path,
                                                    inputFormat = params.table_gg_us_live_item_file_format,
                                                    delimiter = params.table_gg_us_live_item_delimiter,
                                                    schema = schema_gg_us_live_item.dfSchema)

    var dw_lstg_item                = readFilesAsDF(inputPath = params.table_dw_lstg_item_path,
                                                    inputFormat = params.table_dw_lstg_item_file_format,
                                                    delimiter = params.table_dw_lstg_item_delimiter,
                                                    schema = schema_dw_lstg_item.dfSchema)

    val df_gbase_acct_map_catch_all = readFilesAsDF(inputPath = params.table_df_gbase_acct_map_catch_all_path,
                                                    inputFormat = params.table_df_gbase_acct_map_catch_all_file_format,
                                                    delimiter = params.table_df_gbase_acct_map_catch_all_delimiter,
                                                    schema = schema_df_gbase_acct_map_catch_all.dfSchema)

    val df_gbase_acct_map_sub       = readFilesAsDF(inputPath = params.table_df_gbase_acct_map_sub_path,
                                                    inputFormat = params.table_df_gbase_acct_map_sub_file_format,
                                                    delimiter = params.table_df_gbase_acct_map_sub_delimiter,
                                                    schema = schema_df_gbase_acct_map_sub.dfSchema)




    //todo step2 Do some sql work

    //2 dw_lstg_item should firstly filter out all live items using (auct_end_dt > today)
    //3 dw_lstg_item should filter (item_site_id == <site_id>) to filter out one site
    dw_lstg_item = dw_lstg_item
      .filter($"auct_end_dt" >= formatStartDate)
      .filter($"item_site_id".isin(0, 100, 3, 77, 15, 101, 186, 71, 2))//is true?

    dw_lstg_item.show()

    var df_1 = gg_us_live_item.alias("gg").join(dw_lstg_item, gg_us_live_item("itemid") === dw_lstg_item("item_id"), "left")
      .filter(dw_lstg_item("item_id").isNull)
      .select($"gg.*")

    df_1.show()


    //4 output column: item_group_id should set to "0" for all rows
    //5 output column: dw_id should set as current system timestamp using format "yyyyMMddhhmmss00"
    //6 output column: feed_id should map from site_id using map: MappingConst.siteIdGgFeedIdMap
    //7 todo...
    //8 output column: content_language should use MappingConst.siteIdContentLanguageMap
    //9 output column: cntry_code should use MappingConst.siteIdCountryCodeMap
    val setFeedIdFunc = udf { (tgtsiteid: Int) => siteIdContentLanguageMap(tgtsiteid.toString) }
    val setContentLanguageFunc = udf { (itemsiteid: Int) => siteIdContentLanguageMap(itemsiteid.toString) }
    val setCountryCodeFunc = udf { (itemsiteid: Int) => siteIdCountryCodeMap(itemsiteid.toString) }

    df_1 = df_1
        .withColumn("item_group_id", lit(0))
        .withColumn("dw_id", lit(dwId))
        .withColumn("feed_id", setFeedIdFunc($"tgtsiteid"))
        .withColumn("content_language", setContentLanguageFunc($"itemsiteid"))
        .withColumn("cntry_code", setCountryCodeFunc($"itemsiteid"))
        .select($"itemid",$"dw_id",$"feed_id",$"content_language",$"cntry_code")
     // .groupBy($"feed_id").count()
    df_1.show()

    //todo step3 output file
    /*
    df_1.groupBy($"feed_id").count().collect().foreach(
      e => {
        val feedId = e.get(0)
        saveDFToFiles(df = df_1, outputPath = getGGDailyDeleteOutputDir((String.valueOf(feedId).toInt - 1).toString),
                      compressFormat = "gzip", outputFormat = "csv", delimiter = "tab")
      })*/

  }


  def getGGDailyDeleteOutputDir(feedId: String): String = {
    params.outputFilePath + "/" + siteIdNameMap(ggFeedIdSiteIdMap(feedId))
  }
}

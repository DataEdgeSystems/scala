package com.ebay.feeds.delete.daily_delete.google_delete

/**
  * Created by enxie on 2017/10/25.
  */
import scopt.OptionParser

case class Parameter(
                       mode: String = "yarn",
                       currentDate: String = "2099-01-01",

                       table_dw_lstg_item_path: String = "",
                       table_dw_lstg_item_file_format: String = "sequence",
                       table_dw_lstg_item_delimiter: String = "del",

                       table_gg_us_live_item_path:String = "",
                       table_gg_us_live_item_file_format:String = "csv",
                       table_gg_us_live_item_delimiter:String = "del",

                       table_df_gbase_acct_map_catch_all_path:String = "",
                       table_df_gbase_acct_map_catch_all_file_format:String = "csv",
                       table_df_gbase_acct_map_catch_all_delimiter:String = "tab",

                       table_df_gbase_acct_map_sub_path:String = "",
                       table_df_gbase_acct_map_sub_file_format:String = "csv",
                       table_df_gbase_acct_map_sub_delimiter:String = "del",

                       outputFilePath: String = "/apps/hdmi-technology/b_pandaren_kwdm/feed_store/spark/gg_daily_delete",
                       outputFileFormat: String = "csv",
                       outputDelimiter: String = "tab",
                       outputCompressionFormat: String = "gzip"
                     )
object Parameter {

  private lazy val parser = new OptionParser[Parameter]("Feeds GG Daily Delete") {
    head("Feeds GG Daily Delete")

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))
    opt[String]("currentDate")
      .optional
      .valueName("current date, the end point of delete window")
      .action((cont, param) => param.copy(currentDate = cont))
    opt[String]("table_dw_lstg_item_path")
      .optional
      .valueName("table_dw_lstg_item_path")
      .action((cont, param) => param.copy(table_dw_lstg_item_path = cont))
    opt[String]("table_dw_lstg_item_file_format")
      .optional
      .valueName("table_dw_lstg_item_file_format")
      .action((cont, param) => param.copy(table_dw_lstg_item_file_format = cont))
    opt[String]("table_dw_lstg_item_delimiter")
      .optional
      .valueName("table_dw_lstg_item_delimiter")
      .action((cont, param) => param.copy(table_dw_lstg_item_delimiter = cont))

    opt[String]("table_gg_us_live_item_path")
      .optional
      .valueName("table_gg_us_live_item_path")
      .action((cont, param) => param.copy(table_gg_us_live_item_path = cont))

    opt[String]("table_gg_us_live_item_file_format")
      .optional
      .valueName("table_gg_us_live_item_file_format")
      .action((cont, param) => param.copy(table_gg_us_live_item_file_format = cont))

    opt[String]("table_gg_us_live_item_delimiter")
      .optional
      .valueName("table_gg_us_live_item_delimiter")
      .action((cont, param) => param.copy(table_gg_us_live_item_delimiter = cont))

    opt[String]("table_df_gbase_acct_map_catch_all_path")
      .optional
      .valueName("table_df_gbase_acct_map_catch_all_path")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_catch_all_path = cont))

    opt[String]("table_df_gbase_acct_map_catch_all_file_format")
      .optional
      .valueName("table_df_gbase_acct_map_catch_all_file_format")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_catch_all_file_format = cont))

    opt[String]("table_df_gbase_acct_map_catch_all_delimiter")
      .optional
      .valueName("table_df_gbase_acct_map_catch_all_delimiter")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_catch_all_delimiter = cont))

    opt[String]("table_df_gbase_acct_map_sub_path")
      .optional
      .valueName("table_df_gbase_acct_map_sub_path")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_sub_path = cont))

    opt[String]("table_df_gbase_acct_map_sub_file_format")
      .optional
      .valueName("table_df_gbase_acct_map_sub_file_format")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_sub_file_format = cont))

    opt[String]("table_df_gbase_acct_map_sub_delimiter")
      .optional
      .valueName("table_df_gbase_acct_map_sub_delimiter")
      .action((cont, param) => param.copy(table_df_gbase_acct_map_sub_delimiter = cont))



    opt[String]("outputFilePath")
      .optional
      .valueName("outputFilePath")
      .action((cont, param) => param.copy(outputFilePath = cont))

    opt[String]("outputFileFormat")
      .optional
      .valueName("outputFileFormat")
      .action((cont, param) => param.copy(outputFileFormat = cont))

    opt[String]("outputDelimiter")
      .optional
      .valueName("outputDelimiter")
      .action((cont, param) => param.copy(outputDelimiter = cont))

    opt[String]("outputCompressionFormat")
      .optional
      .valueName("outputCompressionFormat")
      .action((cont, param) => param.copy(outputCompressionFormat = cont))

  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
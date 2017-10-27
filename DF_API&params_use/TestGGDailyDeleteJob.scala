package com.ebay.feeds.delete.daily_delete

import com.ebay.feeds.FeedsFunSuite
import com.ebay.feeds.delete.daily_delete.google_delete.GGDailyDeleteJob
import com.ebay.feeds.delete.daily_delete.google_delete.Parameter

/**
  * Created by enxie on 2017/10/25.
  */
class TestGGDailyDeleteJob extends FeedsFunSuite{
  val args = Array(
    "--mode", "local[*]",

    "--table_dw_lstg_item_path", getTestResourcePath("source_tables/daily_delete/dw_lstg_item1.csv"),
    "--table_dw_lstg_item_file_format", "csv",
    "--table_dw_lstg_item_delimiter", "tab",

    "--table_gg_us_live_item_path", getTestResourcePath("source_tables/daily_delete/gg_us_live_item.csv"),
    "--table_gg_us_live_item_file_format", "csv",
    "--table_gg_us_live_item_delimiter", "tab",

    "--table_df_gbase_acct_map_catch_all_path", getTestResourcePath("source_tables/daily_delete/df_gbase_acct_catch_all.csv"),
    "--table_df_gbase_acct_map_catch_all_file_format", "csv",
    "--table_df_gbase_acct_map_catch_all_delimiter", "tab",

    "--table_df_gbase_acct_map_sub_path", getTestResourcePath("source_tables/daily_delete/ddf_gbase_accl_slr.csv"),
    "--table_df_gbase_acct_map_sub_file_format", "csv",
    "--table_df_gbase_acct_map_sub_delimiter", "tab"
  )
  val params = com.ebay.feeds.delete.daily_delete.google_delete.Parameter(args)
  val job = new GGDailyDeleteJob(params,formatStartDate = "2017-10-27")

  override def beforeAll() = {
    addFileToClassLoader(getTestResourceFile("conf/"))
    addFileToClassLoader(getTestResourceFile("conf/source_table_schema/daily_delete/"))

  }

  override def afterAll() = {
    job.stop()
  }

  test("main") {
    job.run()
  }

}

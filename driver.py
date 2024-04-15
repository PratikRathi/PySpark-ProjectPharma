import os
from pyspark.sql.functions import *
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_curr_date
from ingest import load_files, display_df, count_df
from data_processing import data_clean, check_schema
from data_transformation import get_view, zips_count, presc_report, city_with_presc, presc_state_top_5
from saving import save_files
from time import perf_counter
import logging.config
import sys

logging.config.fileConfig('Properties/configuration/logging.config')
root_logger = logging.getLogger()

def main():
    try:
        root_logger.info('I am in the main method')

        root_logger.info('Calling spark object')
        spark = get_spark_object(gav.env, gav.appName)

        root_logger.info('Validating spark object')
        get_curr_date(spark)
        print(spark.conf.set("spark.sql.files.maxPartitionBytes", 20000000))

        for file in os.listdir(gav.src_olap):
            file_dir = gav.src_olap + '/' + file
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        root_logger.info('reading file of format {} from olap'.format(file_format))
        df_city = load_files(spark, file_dir, file_format, header, inferSchema)

        root_logger.info('displaying dataframe {}'.format(df_city))
        display_df(df_city)

        logging.info('validating the dataframe {}'.format(df_city))
        count_df(df_city, 'df_city')

        for file in os.listdir(gav.src_oltp):
            file_dir = gav.src_oltp + '/' + file
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        root_logger.info('reading file of format {} from oltp'.format(file_format))
        df_fact = load_files(spark, file_dir, file_format, header, inferSchema)

        root_logger.info('displaying dataframe {}'.format(df_fact))
        display_df(df_fact)

        logging.info('validating the dataframe {}'.format(df_fact))
        count_df(df_fact, 'df_fact')

        logging.info('implementing data_processing')
        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)
        # df_presc_sel = df_presc_sel.repartition(10, "presc_state", "presc_city")
        # print(spark.conf.get("spark.sql.files.maxPartitionBytes"))
        # print(df_city_sel.rdd.getNumPartitions(), df_presc_sel.rdd.getNumPartitions())
        # df_city_sel.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()
        # df_presc_sel.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()
        display_df(df_city_sel)
        display_df(df_presc_sel)

        logging.info('checking schema')
        check_schema(df_city_sel)
        check_schema(df_presc_sel)

        logging.info('creating view')
        get_view(df_city_sel, df_presc_sel)

        logging.info('Calculating Insights')
        df_report_1 = zips_count(spark)
        presc_report(spark)
        city_with_presc(spark)
        df_report_2 = presc_state_top_5(spark)

        logging.info('saving file')
        save_files(df_report_1, 'orc', gav.city_path, 1, False, 'snappy')
        save_files(df_report_2, 'parquet', gav.presc_path, 2, False, 'snappy')


    except Exception as exp:
        root_logger.error('An error occured when calling main()..', str(exp))
        sys.exit(1)

if __name__ == '__main__':
    start_time = perf_counter()
    main()
    end_time = perf_counter()
    logging.info(f"the process time {end_time - start_time: 0.2f} seconds")
    root_logger.info('Application done')
import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Processing')

def data_clean(df1, df2):
    try:
        logger.warning('data clean method started')
        df_city = df1.select(upper(col('city')).alias('city'), df1.state_id, upper(df1.state_name).alias('state_name'), upper(df1.county_name).alias('county_name'), df1.population, df1.zips)

        df_presc = df2.select(df2.npi.alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'), df2.nppes_provider_first_name.alias('presc_fname'), \
                              df2.nppes_provider_city.alias('presc_city'), df2.nppes_provider_state.alias('presc_state'), df2.specialty_description.alias('presc_speciaility'), \
                              df2.drug_name, df2.total_claim_count, df2.total_day_supply, df2.total_drug_cost, df2.years_of_exp)

        df_presc = df_presc.withColumn('country_name', lit('USA'))
        df_presc = df_presc.withColumn('years_of_exp', regexp_replace('years_of_exp', '[=]', ''))
        df_presc = df_presc.withColumn('years_of_exp', col('years_of_exp').cast('int'))
        df_presc = df_presc.withColumn('presc_name', concat_ws(' ', col('presc_fname'), col('presc_lname')))
        df_presc = df_presc.drop('presc_fname', 'presc_lname')

        df_presc.select([count( when(isnan(c) | col(c).isNull(), c) ).alias(c) for c in df_presc.columns]).show()
        df_presc = df_presc.dropna(subset='presc_id')
        df_presc = df_presc.dropna(subset='drug_name')

        mean_claim_count = df_presc.select(mean(col('total_claim_count'))).collect()[0][0]
        df_presc = df_presc.fillna(mean_claim_count, 'total_claim_count')

    except Exception as e:
        print(str(e))
        logger.error('An error occurred while cleaning the dataframes')
    else:
        logger.warning('data cleaning completed')

    return df_city, df_presc

def check_schema(df):
    try:
        logger.warning('printing schema')
        for i in df.schema.fields:
            logger.info(f"\t{i}")

    except Exception as e:
        logger.error('An error occurred while getting schema')
        raise
    else:
        logger.info('print schema completed')


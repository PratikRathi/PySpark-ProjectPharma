import logging.config

from pyspark.sql.functions import *

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Transformation')

def get_view(df_city, df_presc):
    try:
        logger.warning('creating view from the dataframes')
        df_city.createOrReplaceTempView('city')
        df_presc.createOrReplaceTempView('presc')
    except Exception as e:
        logger.error('an error occured while creating view')
    else:
        logger.warning('views created successfully')

def zips_count(spark):
    try:
        logging.info('Calculating total zips count in city view')
        df = spark.sql("SELECT city, SIZE(SPLIT(zips, ' ')) AS zipscount FROM city")
    except Exception as e:
        print(str(e))
        logger.error('an error occured while calculating zips count')
    else:
        logger.warning('zips count calculated successfully')
        return df

def presc_report(spark):
    try:
        logger.info('Calculating presc aggregations by state, city in presc view')
        spark.sql("SELECT presc_state, presc_city, COUNT(DISTINCT presc_id) AS presc_count, SUM(total_claim_count) AS claim_count \
                  FROM presc \
                  GROUP BY presc_state, presc_city \
                  ORDER BY presc_state, presc_city" \
                  ).show(5)

    except Exception as e:
        print(str(e))
        logger.error('an error occured while calculating presc_report')
    else:
        logger.warning('presc_report calculation successful')

def city_with_presc(spark):
    try:
        logger.info('Calculating cities with prescribers')
        spark.sql("SELECT city.state_name, city.city, COUNT(DISTINCT presc.presc_id) AS presc_count \
                  FROM city INNER JOIN presc ON city.city=presc.presc_city AND city.state_id=presc.presc_state \
                  GROUP BY city.state_name, city.city \
                  ORDER BY city.state_name, city.city" \
                  ).show(5)

    except Exception as e:
        print(str(e))
        logger.error('an error occured while calculating cities with prescribers')
    else:
        logger.warning('cities with prescribers calculation successful')

def presc_state_top_5(spark):
    try:
        logger.info('Calculating presc aggregations by state for top 5 ')
        df = spark.sql("WITH cte AS (SELECT presc_state, presc_id, ROW_NUMBER() OVER (PARTITION BY presc_state ORDER BY SUM(total_claim_count) DESC) AS rank \
                  FROM presc \
                  WHERE years_of_exp>=20 AND years_of_exp<=50 \
                  GROUP BY presc_state, presc_id \
                  ORDER BY 1, 3) \
                  SELECT presc_state, presc_id \
                  FROM cte \
                  WHERE rank<=5" \
                  )

    except Exception as e:
        print(str(e))
        logger.error('an error occured while calculating presc aggregations by state for top 5')
    else:
        logger.warning('presc aggregations by state for top 5 calculation successful')
        return df




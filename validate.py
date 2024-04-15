import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Validate')

def get_curr_date(spark):
    try:
        logger.warning('Started the spark object validation')
        spark.sql("""select current_date""")
    except Exception as e:
        logger.error('An error occured during validation', str(e))
        raise
    else:
        logger.warning('Validation completed')
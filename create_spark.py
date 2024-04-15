from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Create_spark')

def get_spark_object(env, appName):
    try:
        logger.info('Creating spark object')
        if env=='DEV':
            master = 'local'
        else:
            master = 'Yarn'
        logger.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except Exception as exp:
        logger.error('Error occured while creating spark object', str(exp))
        raise
    else:
        logger.info('Spark object created')
        return spark


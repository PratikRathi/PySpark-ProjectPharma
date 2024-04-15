import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_files method started for {}'.format(file_dir))
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).option("header", header).option("inferSchema", inferSchema).load(file_dir)

    except Exception as e:
        logger.error('error occured while loading files {}'.format(file_dir))
        raise
    else:
        logger.warning('dataframe created successfully of {} format'.format(file_format))

    return df


def display_df(df):
    df_show = df.show(5)
    return df_show

def count_df(df, df_name):
    try:
        logger.warning('counting number of records for {}'.format(df_name))
        df_count = df.count()
    except Exception as e:
        logger.error('error while counting the records for {}'.format(df_name))
        raise
    else:
        logger.warning('counted the records for {} -> {} successfully'.format(df_name, df_count))
    return df_count

import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Saving')

def save_files(df, format, filepath, split_no, headerReq, compressionType):
    try:
        logging.warning('saving files started...')
        df.coalesce(split_no).write.mode("overwrite").format(format).save(filepath, header=headerReq, compression=compressionType)
    except Exception as e:
        logger.error('an error occurred while saving the file')
        raise
    else:
        logger.warning('files saved successfully')


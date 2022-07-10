''' Running the Crime ETL application '''
# import argparse
import logging
import logging.config

import yaml

from crime_rate.common.s3 import S3BucketConnector
from crime_rate.common.snowflake import SnowflakeConnector
from crime_rate.transformer.crime_transformer import CrimeETL, CrimeSourceConfig, S3TargetConfig, FileConfig, SnowflakeTargetConfig

'''
Connecting to AWS is needed. In this case, I have set AWS key as environment variable.
Detailed information of my AWS account is (This is an eduation account)
access_key: 'AKIAULQTT7FBEM5ASAN5'
secret_key: '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'
'''

def main(report_result = False):
    '''
        entry point to run the crime ETL job
    '''
    # Parsing YAML file
    # parser = argparse.ArgumentParser(description='Run the Crime ETL job')
    # parser.add_argument('config', help = 'A configuration file in YAML format.')

    # SET your own directory
    config_path = ".\configs\crime_report1_config.yml"
    # Load configure file
    config = yaml.safe_load(open(config_path))

    # configure logging 
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading s3 configuration 
    s3_config = config['s3']
    # reading snowflake configuration 
    sn_config = config['snwflk']
    # creating the S3BucketConnector class instance to connect to S3
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])

    # creating the SnowflakeConnector class instance to connect to snowflake
    sn_connect =  SnowflakeConnector(user =sn_config['user'],
                                      password =sn_config['password'],
                                      account=sn_config['account'],
                                      warehouse=sn_config['warehouse'],
                                      database =sn_config['database'] )

    # reading source configuration
    source_config = CrimeSourceConfig(**config['source'])

    # reading S3 target configuration
    aws_config = S3TargetConfig(**config['aws_target'])

    # reading snowflake target configuration
    sn_load_config = SnowflakeTargetConfig(**config['snw_target'])

    # reading file target configuration
    file_config = FileConfig(**config['trg_format'])

    # readng meta file configuration
    meta_config = config['meta']
    # creating CrimeETL class instance
    logger.info('Crime ETL job started')
    crime_etl = CrimeETL(meta_config['meta_key'], sn_connect,
                        source_config, aws_config, sn_load_config, file_config, s3_bucket_trg)
    # runnig etl job for CrimeETL report1
    crime_etl.etl_report1()
    logger.info('Crime ETL job finished.')


if __name__ == '__main__':
    main()

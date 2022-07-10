''' Xetra ETL Component '''
from typing import NamedTuple
import logging
import pandas as pd

from datetime import datetime
from crime_rate.common.s3 import S3BucketConnector
from crime_rate.common.snowflake import SnowflakeConnector
from crime_rate.common.request_data import Request_Data
from crime_rate.common.meta_process import MetaProcess

class CrimeSourceConfig(NamedTuple):
    '''
    Class for source configuration data

    src_columns: source column names
    src_col_date: column name for date in source -> which month you want 
    src_url_org: the basic url -> to create url
    src_latlong: the latitude and longitude of the need area
    '''
    src_columns: str
    src_col_date: str
    src_url_org: str
    src_latlong: str

class S3TargetConfig(NamedTuple):
    '''
    Class for S3 target configuration data

    s3_trg_name: basic key of target file
    s3_trg_key: the final name of file uploaded to target bucket
    s3_trg_key_date_format: date format of the target file
    '''
    s3_trg_name: str
    s3_trg_key: str
    s3_trg_key_date_format: str


class SnowflakeTargetConfig(NamedTuple):
    '''
    Class for Snowflake configuration data

    sn_from_aws: if load data from AWS S3, True or False
    sn_table_name: target table name in Snowflake
    sn_pattern: which kind of file loading to Snowflake
    sn_stage: the Snowflake stage 
    sn_truncate: if truncate table, True or False
    sn_create: if create new table, True or False
    '''
    sn_from_aws: str
    sn_table_name: str
    sn_pattern: str
    sn_stage: str
    sn_truncate: str
    sn_create: str

class FileConfig(NamedTuple):
    '''
    Class for Snowflake configuration data

    trg_column: rename columns
    trg_file_format: 'csv' or else, in this case, only 'csv' is supported
    trg_date_format: date format
    '''
    trg_column: str
    trg_file_format: str
    trg_date_format: str

class CrimeETL():
    '''
    Reads the Crime data, transforms and writes the transformed to target
    '''

    def __init__(self, meta_key: str, SN_connect:SnowflakeConnector,
                 src_args: CrimeSourceConfig, S3_args, SN_args, file_config:FileConfig, S3_connection = None):
        '''
        Constructor for XetraTransformer
    
        :params request_data: request raw data
        :params S3_connect: connection to target S3 bucket
        :params sn_connect: connection to target Snowflake
        :params meta_key: used as self.meta_key -> key of meta file
        :params src_args: NamedTuple class with source configuration data
        :params trg_args: NamedTuple class with target configuration data
        :params S3_args: NamedTuple class with aws_target configuration data
        :params SN_args: NamedTuple class with snw_target configuration data
        :params file_args: NamedTuple class with trg_format configuration data for file format
        :params urls: created urls to extract data from UK Crime website
        :params latlong: latitude and longitude for needed area
        '''
    
        self._logger = logging.getLogger(__name__)
        self.S3_connect = S3_connection
        self.sn_connect = SN_connect
        self.meta_key = meta_key
        self.src_args = src_args
        self.S3_args = S3_args
        self.SN_args = SN_args
        self.file_args = file_config
        self.urls = Request_Data(self.src_args.src_url_org)
        self.latlong = pd.read_csv(src_args.src_latlong)

    def extract(self, urls):
        '''
        Read the source data and concatenates them to one Pandas DataFrame

        :returns:
            data_frame: Pandas DataFrame with the extracted data
        '''
        self._logger.info('Extracting Crime source files started...')
        # data_frame = pd.concat([Request_Data.data_from_url(url) for url in urls], ignore_index=True)
        data_frame = Request_Data.data_from_url(urls)
        self._logger.info('Extracting Crime source files finished')
        return data_frame
   

    def transform_report1(self, data_frame:pd.DataFrame):
        '''
        Applies the necessary transformation to create report 1

        :param data_frame: Pandas DataFrame as Input

        :returns:
            data_frame: Transformed Pandas DataFrame as Output
        '''
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied')
            return data_frame
        self._logger.info('Applying transformations to Crime source data for report 1 started...')
        df_new = data_frame.loc[:,self.src_args.src_columns]
        # Rename columns
        df_new = df_new.rename(columns = self.file_args.trg_column)
        df_new['month'] = pd.to_datetime(df_new['month']).dt.date
        df_new.drop_duplicates(keep='last')
        df_new.dropna(subset=['street_id'],inplace=True)

        self._logger.info('Applying transformation to Crime source data finished...')
        return df_new

    def load_to_snowflake(self, data_frame=None):
        if self.SN_args.sn_create==False:
            self.sn_connect.upload_to_sn(self.SN_args.sn_table_name, 
                    self.SN_args.sn_create, self.SN_args.sn_truncate, self.SN_args.sn_from_aws,
                    self.SN_args.sn_stage, self.SN_args.sn_pattern)      
        else:
            self.sn_connect.upload_to_sn(self.SN_args.sn_table_name, 
                    self.SN_args.sn_create, self.SN_args.sn_truncate, self.SN_args.sn_from_aws,
                    self.SN_args.sn_stage, self.SN_args.sn_pattern, data_frame)
        self._logger.info('Table %s has been created in snowflake',self.SN_args.sn_table_name)

    def load_to_s3(self, data_frame):
        '''
        Saves a Pandas DataFrame to the target

        :param data_frame: Pandas DataFrame as Input
        '''
        # Creating target key
        target_key = (
            f'{self.S3_args.s3_trg_key}'
            f'{datetime.today().strftime(self.S3_args.s3_trg_key_date_format)}'
            f'{self.file_args.trg_file_format}'
        )
        # Writing to target S3 bucket
        self.S3_connect.write_df_to_s3(data_frame, target_key , self.file_args.trg_file_format)
        self._logger.info('Crime target data successfully written to S3.')
        # Updating meta file
        MetaProcess.update_meta_file(self.src_args.src_col_date, self.meta_key, self.S3_connect)
        self._logger.info('Crime meta file successfully updated to S3.')
        
        # Loading data into Snowflake
        # If creat new table is needed
        return True
        

    def etl_report1(self):
        '''
        Extract, transform and load to create report 1
        '''
        if MetaProcess.check_date(self.src_args.src_col_date, self.meta_key, self.S3_connect, self.file_args.trg_date_format):
            # Extraction
            if self.src_args.src_col_date:
                urls = self.urls.get_url_custom(self.latlong, self.src_args.src_col_date)
            else: 
                urls = self.urls.get_url_custom(self.latlong)
            df = self.extract(urls)
            # Transformation
            df = self.transform_report1(df)
            # Load
            self.load_to_s3(df)
            self.load_to_snowflake()
            print('Success')
            return True
        else: 
            self.load_to_snowflake()
            return False
    
































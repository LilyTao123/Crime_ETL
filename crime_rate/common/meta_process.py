'''
Methods for processing the meta file
'''
from datetime import datetime
import collections
import pandas as pd
from crime_rate.common.constants import MetaProcessFormat
from crime_rate.common.custom_exceptions import WrongMetaFileException

from crime_rate.common.s3 import S3BucketConnector

class MetaProcess():
    '''
    Class for working with the meta file
    '''
    @staticmethod
    def update_meta_file(date:list, meta_key:str, s3_bucket_meta:S3BucketConnector):
        """
        Updating the meta file with processed Xetra dates and todays date as processed date

        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta ->S3BucketConnector for the bucket with the meta file 
        """
        # Creating an empty DataFrame using the meta file columm names
        df_new =  pd.DataFrame(columns = [
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = [date]
        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            [datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)]
        try:
            # If meta file exists -> union DataFrame of old and new meta data is created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all =  pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        # Writing to S3
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def check_date(arg_date, meta_key, s3_bucket_meta, trg_format):
        '''
        Check if the requested date has been done

        :param: arg_date -> the requested date
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        :param: trg_format -> the format of date

        returns:
            True: It's a new work
            False: The requested date has been done
        '''
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            
            # Creating a list of dates from first_date until today
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key,encoding = 'utf-8', sep = ',')
            date = datetime.strptime(arg_date, trg_format).date()
            src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
            if date in src_dates:
                print('The requested data already existed.')
                return False
            else:
                print('ETL process start.')
            return True
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file found -> creating a date list from first_date - 1 day until today 
            return True
        


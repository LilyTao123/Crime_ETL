'''Connector and methods accessing S3'''
import logging
import pandas as pd

from sqlalchemy import create_engine

class SnowflakeConnector():
    ''' 
    Class for interacting with Snowflake database
    '''
    def __init__(self, user: str, password: str, account: str, warehouse: str, database: str):
        ''' 
        Constructor for S3BucketConnector

        : param access_key: access key for accessing S3
        : param secret_key: secret key for accessing S3
        : param endpoint_url: endpoint url to S3
        : param bucket: S3 bucket name 
        '''
        logging.disable('DEBUG')
        self._logger = logging.getLogger(__name__)
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        # self._conect_snw = snowflake.connector.connect(
        #                     user=self.user,
        #                     password=self.password,
        #                     account = self.account,
        #                     warehouse=self.warehouse,
        #                     database=self.database
        #                     )
        # self.cs = self._conect_snw.cursor()
        self.conn_string = f"snowflake://{self.user}:{self.password}@{self.account}/{self.database}/{'PUBLIC'}"
        self.create_engine = create_engine(self.conn_string)


    def fetch_pandas_old(self, sql):
        sql1 = 'USE CRIME_UK;'
        engine = self.create_engine
        rows = 0
        with engine.connect() as con:
            con.execute(sql1)
            con.execute(sql)
            while True:
                dat = con.fetchmany(50000)
                if not dat:
                    break
                df = pd.DataFrame(dat, columns=con.description)
                rows += df.shape[0]
        return df
    
    '''
    Upload data to snowflake database
    '''
    def upload_to_sn(self,table_name, create, truncate, s3=None, stage=None,pattern=None, data_frame=None):
        engine = self.create_engine
        self._logger.info('Connecting to Snowflake')
        with engine.connect() as con:
            if create:
                data_frame.head(0).to_sql(name = table_name, 
                                      con=con, 
                                      if_exists="replace", 
                                      index=False)
                self._logger.info('Created a table %s', table_name)
            if truncate:
                con.execute(f"truncate table {table_name}")
                self._logger.info('Truncated a table %s', table_name)   
            if s3:
                with engine.connect() as con:
                    con.execute(f"copy into {table_name} from @{stage} \
                        FILE_FORMAT=(TYPE = 'CSV' SKIP_HEADER = 1)\
                        PATTERN= {pattern};")
                self._logger.info('Loading table %s into Snowflake', table_name)
                return True
            # else:
            #     file_name = f"{table_name}.csv"
            #     file_path = os.path.abspath(file_name)
            #     data_frame.to_csv(file_path, index=False, header=False)
            #     con.execute(f"put file://{file_path}* @%{table_name}")
            #     con.execute(f"copy into {table_name}") 
            #     return True
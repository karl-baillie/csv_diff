import logging
import os

import dask.dataframe as dd
import pandas as pd
import dask


logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=os.environ.get("LOGLEVEL", "INFO"),
        datefmt='%Y-%m-%d %H:%M:%S'
    )

logging.info("Starting")
df=pd.DataFrame()
# df=pd.read_csv(
#     # '/Users/karl.baillie/Downloads/IPQS-Proxy-Database-Arkose.csv.gz',
#      "./test_data/old.csv",

#     compression='gzip',header=0, sep=',', quotechar='"',
#     dtype={'RangeType':'string', 'IsVPN':'string', 'IsTOR':'string', 'Country':'string',
#            'City':'string', 'Region':'string', 'ISP':'string', 'Organization':'string',
#            'ASN':'string', 'Timezone':'string', 'Latitude':'string', 'Longitude':'string'}
# )
# df=pd.read_csv(
#     './test_data/IPQS-sample-database.csv',
#     dtype={'RangeType':'string', 'IsVPN':'string', 'IsTOR':'string', 'Country':'string',
#            'City':'string', 'Region':'string', 'ISP':'string', 'Organization':'string',
#            'ASN':'string', 'Timezone':'string', 'Latitude':'string', 'Longitude':'string'},
# )

df_old = dd.read_csv(
    #"/Users/karl.baillie/Downloads/IPQS-Proxy-Database-Arkose.csv",
    "./test_data/old.csv",
    # compression='gzip',
    dtype={'RangeType':'string', 'IsVPN':'string', 'IsTOR':'string', 'Country':'string',
           'City':'string', 'Region':'string', 'ISP':'string', 'Organization':'string',
           'ASN':'string', 'Timezone':'string', 'Latitude':'string', 'Longitude':'string'}
)

df_old['type'] = "old"

print(df_old.head())

df_new = dd.read_csv(
    # "/tmp/new-arkose.csv",
    "./test_data/new.csv",
    # compression='gzip',
    dtype={'RangeType':'string', 'IsVPN':'string', 'IsTOR':'string', 'Country':'string',
           'City':'string', 'Region':'string', 'ISP':'string', 'Organization':'string',
           'ASN':'string', 'Timezone':'string', 'Latitude':'string', 'Longitude':'string'}
)

df_new['type'] = "new"
print(df_new.head())

dd_concat = dd.concat([df_old, df_new])


#, axis=0, join='outer', ignore_index=False, keys=None,
#          levels=None, names=None, verify_integrity=False, copy=True)
#dd.merge(df_new, df_old, left_index=True, right_index=True)

print(dd_concat.head(50))
logging.info("Finished reading new csv into DF")
# logging.info("Saving Parquet to disk")
# df.to_parquet('./test-data/old.parquet', engine='fastparquet')
# logging.info("Finished saving Parquet to disk")
#df.to_csv('new.csv')
import os,base64
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, date, timedelta
import urllib.request as urllib2
import tempfile
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import duckdb 
import re ,shutil
from urllib.request import urlopen

st.set_page_config(
    page_title="Example of Delta Table and DuckDB",
    page_icon="✅",
    layout="wide",
)
table_path = "xxx/"
st_autorefresh(interval=4 * 60 * 1000, key="dataframerefresh")

# dashboard title
st.title("Example of Delta Table and DuckDB, Auto refresh every 5 minutes")

col1, col2 = st.columns([1, 1])

########################################################## Query arrow table as an ordinary SQL Table#####################################
try:
   dt = DeltaTable(table_path).to_pyarrow_table()  
con = duckdb.connect()
results =con.execute('''
with xx as (Select SETTLEMENTDATE, (SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE , DUID,MIN(SCADAVALUE) as mwh from  dt group by all)
Select SETTLEMENTDATE,LOCALDATE, sum(mwh) as mwh from  xx group by all order by SETTLEMENTDATE desc
''').arrow()
results = results.to_pandas()
column = results["SETTLEMENTDATE"]
now = str (column.max())
st.subheader("Latest Updated: " + now)

############################################################# Visualisation ##############################################################
#localdate is just a stuid hack, Javascript read datetime as UTC not local time :(
import altair as alt
c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mwh:Q',
                                          tooltip=['LOCALDATE','mwh']).properties(
                                            width=1200,
                                            height=400)
st.write(c)
df=results[['SETTLEMENTDATE','mwh']]
st.dataframe(df)

###########################################################Buttons and Links #############################################################
#Download Button


def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(df)
col2.download_button(
     label="Download data as CSV",
     data=csv,
     file_name='large_df.csv',
     mime='text/csv',
 )


link='[Data Source](http://nemweb.com.au/Reports/Current/Dispatch_SCADA/)'
col1.markdown(link,unsafe_allow_html=True)

link='[Blog](https://datamonkeysite.com/2022/06/28/using-delta-lake-with-python/)'
col1.markdown(link,unsafe_allow_html=True)

####################################### ETL#############################################################################
#st.subheader("Downloading New files from AEMO website, Data will be refreshed in 5 minutes, or refresh your browser ")
def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)

def getfiles(Path,url):    
    
    
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist1 = pattern.findall(result)
    filelist_unique = dict.fromkeys(filelist1)
    filelist_sorted=sorted(filelist_unique, reverse=True)
    filelist = filelist_sorted[:288]
    
    table_path = Path 
    try:
        df = DeltaTable(table_path).to_pandas()
    except:
        df=pd.DataFrame(columns=['file']) 
    
    df= df['file'].unique()
    #print (df)

    current = df.tolist()
    #print(current)

    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    return files_to_upload


def load(files_to_upload,table_path,url): 
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
            df=df.dropna(how='all') #drop na
            df['SETTLEMENTDATE']= pd.to_datetime(df['SETTLEMENTDATE'])
            df['Date'] = df['SETTLEMENTDATE'].dt.date
            df['file'] = x
            tb=pa.Table.from_pandas(df,preserve_index=False)
            my_schema = pa.schema([
                      pa.field('SETTLEMENTDATE', pa.timestamp('us')),
                      pa.field('DUID', pa.string()),
                      pa.field('SCADAVALUE', pa.float64()),
                      pa.field('Date', pa.date32()),
                      pa.field('file', pa.string())
                      ]
                                                       )
            xx=tb.cast(target_schema=my_schema)
            #print(xx)
            write_deltalake(table_path, xx,mode='append',partition_by=['Date'])


url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
files_to_upload=getfiles(table_path,url)
load(files_to_upload,table_path,url)
#st.write(files_to_upload)


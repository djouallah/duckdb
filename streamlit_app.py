import os,base64
import streamlit as st
from datetime import datetime, date, timedelta
import urllib.request as urllib2
import tempfile
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import re ,shutil
from urllib.request import urlopen
col1, col2 = st.columns([1, 1])


def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def load(Path):    
    
    url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
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
    st.write(str(len(files_to_upload)) + ' New File Loaded')
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
            
import duckdb 
from deltalake import DeltaTable
# Define the Path to your Delta Table.
table_path = "xxx/"
load(table_path)
# Get table as pyarrow table
dt = DeltaTable(table_path).to_pyarrow_table()

# Query arrow table as an ordinary SQL Table.

con = duckdb.connect()
results =con.execute('''
with xx as (Select SETTLEMENTDATE, DUID,MIN(SCADAVALUE) as mwh from  dt group by all)
Select SETTLEMENTDATE, sum(mwh) as mwh from  xx group by all order by SETTLEMENTDATE desc
''').arrow()
results = results.to_pandas()
column = results["SETTLEMENTDATE"]
now = str (column.max())
st.subheader("Nem  Today: " + now)


import altair as alt
c = alt.Chart(results).mark_area().encode( x=alt.X('SETTLEMENTDATE:T',timeUnit='yearmonthdatehoursminutesseconds')), y='mwh:Q',tooltip=['SETTLEMENTDATE', 'mwh']).interactive()
st.write(c)
#download
def download_link(object_to_download, download_filename, download_link_text):
    """
    Generates a link to download the given object_to_download.
    object_to_download (str, pd.DataFrame):  The object to be downloaded.
    download_filename (str): filename and extension of file. e.g. mydata.csv, some_txt_output.txt
    download_link_text (str): Text to display for download link.
    Examples:
    download_link(YOUR_DF, 'YOUR_DF.csv', 'Click here to download data!')
    download_link(YOUR_STRING, 'YOUR_STRING.txt', 'Click here to download your text!')
    """
    if isinstance(object_to_download,pd.DataFrame):
        object_to_download = object_to_download.to_csv(index=False)

    # some strings <-> bytes conversions necessary here
    b64 = base64.b64encode(object_to_download.encode()).decode()

    return f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'


col1.button("Refresh")

tmp_download_link = download_link(results, 'YOUR_DF.csv', 'Export results')
col2.markdown(tmp_download_link, unsafe_allow_html=True)

link='[Blog](https://datamonkeysite.com/2022/06/28/using-delta-lake-with-python/)'
col2.markdown(link,unsafe_allow_html=True)

link='[Data Source](http://nemweb.com.au/Reports/Current/Dispatch_SCADA/)'
col1.markdown(link,unsafe_allow_html=True)

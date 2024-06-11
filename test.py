import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer, DATE
to_day = date.today()


pd.set_option('display.float_format','{:.2f}'.format)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 1000)
engine =create_engine("mariadb+mariadbconnector://root:lawr785633@127.0.0.1:3306/mydb")
url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=csv&d={113/06/07}&s=0,asc,0"

df = pd.read_csv(url,
                
                 header=2,
                 skipfooter=8,
                 usecols=[0,1,2,4,5,6,7,8],
                 thousands=",",
                 encoding="cp950",
                #  true_values=["yes"],
                #  false_values=["no"],
                 engine='python'              
                 
                 )

df.columns = ['stockid','stockname','over','open','high','low','bef','volume']
df['up_date'] = to_day

# print(df.head())
# print(df.info())
# quit()

# df['high'] = str(df['high']).strip()
# df['low'] = str(df['low']).strip()
# df['over'] = str(df['over']).strip()
# df['volume'] = str(df['volume']).strip()
# quit()

# print(df.head())
# print(df.info())
# quit()





# print(df)
# print(df.info())
# quit()

linnum = len(df)

my_list = []
for i in range(linnum):
    if len(df.iloc[i,0]) == 4 :
        
        my_list.append(df.iloc[i])
    else:
        continue

df1 = pd.DataFrame(my_list)       
df1.loc[df1['high']=="---",["over","open","high","low"]] = df1.loc[df1["high"]=="---","bef"]
df1.loc[df1['volume']=="---","volume"] = 0
# drop=true把舊id去掉
df1 = df1.reset_index(drop=True)

df1 = df1.astype(
    {
        "stockid" : "int16",
        "stockname" : "category",
        "over" : "float32",
        "open" : "float32",
        "high" : "float32",
        "low" : "float32",
        "bef" : "float32",
        "volume" : "float32",
        "up_date" : "datetime64[ns]"
    }
)
# print(df1)
# print(df1.info())
# print(my_list)
# quit()

dtypedict = {
        'stockid':Integer,
        'stockname':NVARCHAR(length=100),
        'over':Float,
        'open':Float,        
        'high':Float,
        'low':Float,
        'bef':Float,
        'volume':Integer,
        'up_date': DATE,

            }


def IntoTable(row):
    row_pd = pd.DataFrame([row])
    row_pd.to_sql(f'p_{row_pd.iloc[0,0]}', engine, if_exists='append', dtype=dtypedict, index=False)

df1.apply(IntoTable,axis=1)


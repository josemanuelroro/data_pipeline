import pandas as pd
df=pd.read_csv("/data/scrapper_vol/mercados/eroski.csv",sep="|")
df= df.replace({r'\r+|\n+|\t+':'',r'|':r'',r'\\':'/'}, regex=True) #borramos el caracter de retorno
df.to_csv("/data/scrapper_vol/mercados/eroski.csv",sep="|",index=False)
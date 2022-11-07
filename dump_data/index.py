import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:admin@localhost:5432/nhannt')

df = pd.read_csv('tbl_Country.csv', encoding='ISO-8859-1')
df.to_sql('tbl_Country', engine)

# tbl_Sked_Detail.csv
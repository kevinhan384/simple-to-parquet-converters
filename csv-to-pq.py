import polars as pl
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser(prog='csv-to-pq',
                                 description='Converts csv to parquet for swallow events')

parser.add_argument('infile', help="foo.csv")
parser.add_argument('outfile', help="bar.parquet")
parser.add_argument('starttime', help='mm/dd/yy-HH:MM:SS')

args = parser.parse_args()

df = pl.read_csv(args.infile)

times: list[str] = df["Time"].to_list()
start: str = df["Date Created"].to_list()[0]

df_list = []
for t in times:
    t = t.strip()
    elements = t.split(' ')
    if "ns" in t:
        element = float(elements[0]) * 1.6667e-11
    elif "sec" in t:
        element = float(elements[0]) * 1 / 60
    else:
        element = float(elements[0])

    element = datetime.strptime(args.starttime, "%m/%d/%y-%H:%M:%S") + timedelta(minutes=element)

    df_list.append([element, 1])

df = pl.DataFrame(df_list, schema=['ts', 'mark'])
df.write_parquet(args.outfile, use_pyarrow=True)
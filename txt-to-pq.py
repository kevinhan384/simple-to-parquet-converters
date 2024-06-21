import polars as pl
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser(prog='txt-to-pq',
                                 description='Converts txt to parquet for swallow data')

parser.add_argument('infile', help="foo.txt")
parser.add_argument('outfile', help="bar.parquet")
parser.add_argument('starttime', help='mm/dd/yy-HH:MM:SS')

args = parser.parse_args()

file = open(args.infile, "r")

line_list = []

for line in file.readlines():
    elements_list = []
    elements = line.split('\t')
    elements.pop()

    if len(elements) != 4:
        continue

    for element in elements:
        try:
            element = float(element.strip())
            elements_list.append(element)
        except:
            break
    
    try:
        elements_list[0] = datetime.strptime(args.starttime, "%m/%d/%y-%H:%M:%S") + timedelta(minutes=elements_list[0])
    except:
        continue

    line_list.append(elements_list)

df = pl.DataFrame(line_list, schema=["ts", "CH1", "CH2", "CH5"])
df.write_parquet(args.outfile, use_pyarrow=True)

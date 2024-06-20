import pandas as pd
from datetime import datetime, timedelta

infile = input("Input file> ")
outfile = input("Output file> ")
starttime = input("Starttime (mm/dd/yy HH:MM:SS)> ")

df = pd.read_csv(infile)

times: list[str] = df["Time"].tolist()
start: str = df["Date Created"].tolist()[0]

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

    element = datetime.strptime(starttime, "%m/%d/%y %H:%M:%S") + timedelta(minutes=element)

    df_list.append([element, 1])

df = pd.DataFrame(df_list, columns=["min", "mark"])
df.to_parquet(outfile, engine="pyarrow")
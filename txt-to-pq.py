import pandas as pd
from datetime import datetime, timedelta

in_filename = input("Input> ")
out_filename = input("Output> ")
starttime = input("Starttime (mm/dd/yy HH:MM:SS)> ")
file = open(in_filename, "r")

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
        elements_list[0] = datetime.strptime(starttime, "%m/%d/%y %H:%M:%S") + timedelta(minutes=elements_list[0])
    except:
        continue

    line_list.append(elements_list)

df = pd.DataFrame(line_list, columns=["min", "CH1", "CH2", "CH5"])

df.to_parquet(out_filename, engine="pyarrow")

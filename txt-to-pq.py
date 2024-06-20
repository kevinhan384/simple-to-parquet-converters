import pandas as pd
from datetime import datetime, timedelta

in_filename = input("Input filename> ")
out_filename = input("Output filename> ")
file = open(in_filename, "r")

line_list = []

for line in file.readlines():
    elements_list = []
    elements = line.split('\t')
    elements.pop()

    for element in elements:
        element = float(element.strip())
        elements_list.append(element)
    
    elements_list[0] = datetime(2000, 1, 1) + timedelta(elements_list[0])

    line_list.append(elements_list)

df = pd.DataFrame(line_list, columns=["min", "CH1", "CH2", "CH5"])

df.to_parquet(out_filename, engine="pyarrow")
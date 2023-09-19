import pandas as pd
import sys
import numpy as np

file = "04.04.2023"
if len(sys.argv) > 1:
    file = sys.argv[1]

filename = f"files/{file}.xlsx"

# Get meta information
xl = pd.ExcelFile(filename)
sheets = xl.sheet_names

config = {
    "skip_sheets": [0, 6, 7],
    "meta_rows": {
        1: [range(4), range(-2, 0)],
        2: [range(4), range(-2, 0)],
        3: [range(4), range(-2, 0)],
        4: [range(4), range(-2, 0)],
        5: [range(4), range(-2, 0)],
    },
    "remove_empty_rows": True,
    "remove_empty_cols": True,
    "split_dfs": {1: True, 2: True, 3: True, 4: True, 5: True},
}

def consecs(ll):
    ol = []
    cfound = False
    for i, l in enumerate(ll):
        if i<len(ll)-1 and l == ll[i+1]-1:
                cfound = True
                continue
        elif cfound==True:
             ol.append(l)
             cfound = False
    return ol

def boundaries(a, start, end):
    ranges = []   
    for i in a:
        if start != i:
            ranges.append([start, i])
        start  = i + 1
    if start < end:
        ranges.append([start, end])
    return ranges

dfs = []
meta_rows = [list() for x in sheets]  
for index, sheet in enumerate(sheets):
    if index in config["skip_sheets"]:
        continue

    df = xl.parse(sheet, header=None)
    if index in config["meta_rows"].keys():
        for rows in config["meta_rows"][index]:
            meta_rows[index].append(df.iloc[rows])
            df.drop(df.index[rows], inplace=True)

    # if config["remove_empty_rows"]:
    #     df.dropna(axis='columns', how='all', inplace=True)
    # if config["remove_empty_cols"]:
    #     df.dropna(axis='rows', how='all', inplace=True)
    
    df.reset_index(drop=True, inplace=True)
    df.columns = range(df.columns.size)

    if index in config["split_dfs"].keys():
        df["nancnt"] = df.isnull().sum(axis=1)
        maxcnt = max(df["nancnt"])
        maxrows = df.index[df["nancnt"] >= maxcnt].to_list()   
        df.drop(columns=["nancnt"], inplace=True)     
        splitindex = consecs(maxrows)
        boxes = boundaries(splitindex, 0, df.shape[0])

        for b in boxes:
            tdf = df.iloc[b[0]:b[1],:]
            if tdf.size > 1:
                print(tdf.size)
                dfs.append({"sheet":sheet, "index":index,"df":tdf})

        input("Enter any key...")
    else:
        dfs.append({"sheet": sheet, "index": index, "df": df})

for index, dfo in enumerate(dfs):
    print("saving sheet", dfo["sheet"])
    dfo["df"].dropna(axis='columns', how='all', inplace=True)
    dfo["df"].dropna(axis='rows', how='all', inplace=True)
    dfo["df"].to_csv(f"changes_output/{file}-{dfo['sheet']}-{index}.csv", index=None, header=None)

# for index, dfo in enumerate(meta_rows):
#     print("saving meta", index)
#     dfo.dropna(axis='columns', how='all', inplace=True)
#     dfo.dropna(axis='rows', how='all', inplace=True)
#     dfo.to_csv(f"changes_output/{file}-meta-{index}.csv", index=None, header=None)

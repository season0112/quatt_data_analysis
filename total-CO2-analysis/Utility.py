import pandas as pd
import glob


def MergeCSVFile(path):

    csv_files = glob.glob( path + '*.csv')

    df_list = [pd.read_csv(file) for file in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)

    merged_df['Date'] = pd.to_datetime(merged_df['Date'])

    sorted_df = merged_df.sort_values(by='Date')

    sorted_df.to_csv(path + 'MergedFile.csv', index=False)





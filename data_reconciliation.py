import pandas as pd

aurora_file = "table_counts_aurora.csv"
athena_file = "table_counts_athena.csv"


def do_compare(df1: pd.DataFrame, df2: pd.DataFrame):
    # df1['counts'] = df1['counts'].astype(int)

    # print(df1['table_name'], df1['counts'].sub(df2['counts']))

    pass


if __name__ == "__main__":
    df_aurora = pd.read_csv(aurora_file, header='infer').sort_values(by=['table_name'])
    df_athena = pd.read_csv(athena_file, header='infer').sort_values(by=['table_name'])

    result = pd.concat([df_aurora, df_athena]).sort_values(by=['table_name'])

    print(result)

import pandas as pd
import json


def main():
    df_sales = pd.read_csv('data/sales_data.csv')
    print(df_sales.head())
    f = open('cred.json')
    data = json.load(f)
    api_key = data['api_key']
    print(api_key)

#if __name__ == '__main__':
#    main()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/

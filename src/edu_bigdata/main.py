from dataweb import DataWeb
import pandas as pd



def main():
    dataweb = DataWeb()
    df = dataweb.obtener_datos()
    df = dataweb.convertir_numericos(df)
    df.to_csv("src/edu_bigdata/static/csv/data_web.csv", index=False) #/workspaces/bigdata_2025_1_2/src/edu_bigdata/static/csv



if __name__ == "__main__":
    main()

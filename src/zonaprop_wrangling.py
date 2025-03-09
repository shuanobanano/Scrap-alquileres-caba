import pandas as pd
import numpy as np

def _create_currency_column(df:pd.DataFrame) -> pd.DataFrame:
    conditions_currency = [
        df['Price'].str.startswith('USD'),
        df['Price'].str.startswith('$') | df["Price"].str.startswith("Pesos"),
        df['Price'].str.startswith('Consultar')
    ]

    values = ['USD', 'ARS', 'Consultar']

    df['currency'] = np.select(conditions_currency, values, default="Unknown")
    return df


def _split_features(features):
    metros_cuadrados = np.nan
    ambientes = np.nan
    dormitorios = np.nan
    baños = np.nan
    cocheras = np.nan

    for feature in features:
        if 'm²' in feature:
            metros_cuadrados = int(feature.split()[0])
        elif 'amb.' in feature:
            ambientes = int(feature.split()[0])
        elif 'dorm.' in feature:
            dormitorios = int(feature.split()[0])
        elif 'baños' in feature:
            baños = int(feature.split()[0])
        elif 'coch.' in feature:
            cocheras = int(feature.split()[0])

    return pd.Series([metros_cuadrados, ambientes, dormitorios, baños, cocheras])

def _clean_number_with_text_column(serie:pd.Series) -> pd.Series:
    serie = serie.str.replace(r'[^\d\.]', '', regex=True)\
        .str.replace('.','')\
        .replace('', np.nan)\
        .astype(float)
    return serie

def main_zonaprop_wrangling(df:pd.DataFrame):
    df = _create_currency_column(df)
    df[['metros_cuadrados', 'ambientes', 'dormitorios', 'baños', 'cocheras']] = df['Features'].apply(_split_features)
    df = df.drop("Features", axis=1)
    df["Price"] = _clean_number_with_text_column(df["Price"])
    df["Expensas"] = _clean_number_with_text_column(df["Expensas"])
    df["Alquiler / m2"] = df["Price"] / df["metros_cuadrados"]
    return df
    
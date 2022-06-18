import gspread
import ast
import pandas as pd
import pygsheets
import numpy as np
import time


class Translator:
    """ A class that translate tabular data. """

    def __init__(self, df, name):
        self.name = name
        self.df = df
        self.gc = pygsheets.authorize(
            service_file='pipeline/common/servicefile.json')
        self.wks = None

    def add_df_as_worksheet(self):
        # Apply translation function to df items
        df = self.df.applymap(
            lambda x: x.replace('"', '') if (
                type(x) != bool and x != '' and type(x) != float and x != None) else x
        )
        df = df.applymap(
            lambda x: f'=GOOGLETRANSLATE("{x}")' if (
                type(x) != bool and x != '') else x
        )
        df = df.where(pd.notnull(df), None)
        df = df.replace({np.nan: None})
        # open the google spreadsheet (where 'translations' is the name of my sheet)
        sh = self.gc.open('translations')
        try:
            self.wks = sh.add_worksheet(self.name)
            self.wks.set_dataframe(df, (1, 1))
        except Exception as e:
            print('Error: worksheet already exists, replacing with new one...')
            wk = sh.worksheet_by_title(self.name)
            sh.del_worksheet(wk)
            self.wks = sh.add_worksheet(self.name)
            self.wks.set_dataframe(df, (1, 1))

    def still_loading(self, df):
        still_loading = False
        for column in df.columns:
            loading = df[column].astype(str).str.contains('Loading...').any()
            if loading:
                still_loading = True
                break
        return still_loading

    def translate(self):
        """
        Uploads sheet to gsheets, translates, returns dataframe.
        """
        self.add_df_as_worksheet()
        df = self.wks.get_as_df()
        while self.still_loading(df):
            print('Still translating')
            time.sleep(10)
            df = self.wks.get_as_df()
        try:
            df['confirmedHours'] = df['confirmedHours'].apply(
                lambda x: ast.literal_eval(x.title()))
        except Exception as e:
            print(f'Error: missing column {e}')
        df = df.replace('', np.nan)
        df = df.replace('None', np.nan)
        return df

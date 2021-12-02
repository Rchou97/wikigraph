# libraries for gathering wiki histories & MapReduce
import wikipedia_histories as hist
import pandas as pd
import numpy as np

class Data:

    #ana_df: pd.DataFrame
    bus_df: pd.DataFrame
    #bus_ana_df: pd.DataFrame
    comp_df: pd.DataFrame
    data_df: pd.DataFrame
    #data_war_df: pd.DataFrame
    mark_df: pd.DataFrame
    #mark_res_df: pd.DataFrame
    open_df: pd.DataFrame
    product_df: pd.DataFrame
    master_df: pd.DataFrame


    def clean_data(self):
        # Aligning data 
        self.bus_df = self.bus_df[2532:].reset_index(drop = True)
        self.comp_df = self.comp_df[650:].reset_index(drop = True)
        self.data_df = self.data_df[4206:].reset_index(drop = True)
        self.mark_df = self.mark_df[114:].reset_index(drop = True)
        self.open_df = self.open_df[1002:].reset_index(drop = True)

        # Filtering the columns to three columns
        #self.ana_df = self.ana_df[['revid', 'time', 'text']]
        self.bus_df = self.bus_df[['revid', 'time', 'text']]
        #self.bus_ana_df = self.bus_ana_df[['revid', 'time', 'text']]
        self.comp_df = self.comp_df[['revid', 'time', 'text']]
        self.data_df = self.data_df[['revid', 'time', 'text']]
        #self.data_war_df = self.data_war_df[['revid', 'time', 'text']]
        self.mark_df = self.mark_df[['revid', 'time', 'text']]
        #self.mark_res_df = self.mark_res_df[['revid', 'time', 'text']]
        self.open_df = self.open_df[['revid', 'time', 'text']]
        self.product_df = self.product_df[['revid', 'time', 'text']]

        # Merge dataframes
        self.merge = [self.bus_df, self.comp_df, self.data_df, self.mark_df, self.open_df, self.product_df]
        self.master_df = pd.concat(self.merge).reset_index(drop = True)

    def __init__(self):
        # Analytics
        #self.ana = hist.get_history("Analytics")
        #self.ana_df = hist.to_df(self.ana)

        # Business intelligence
        self.bus_intel = hist.get_history("Business intelligence")
        self.bus_df = hist.to_df(self.bus_intel)

        # Business analysis
        #self.bus_ana = hist.get_history("Business analysis")
        #self.bus_ana_df = hist.to_df(self.bus_ana)

        # Competitive intelligence
        self.comp_intel = hist.get_history("Competitive intelligence")
        self.comp_df = hist.to_df(self.comp_intel)

        # Data mining
        self.data_min = hist.get_history("Data mining") 
        self.data_df = hist.to_df(self.data_min)

        # Data warehouse
        #self.data_war = hist.get_history("Data warehouse")
        #self.data_war_df = hist.to_df(self.data_war)
        
        # Market intelligence
        self.mark_intel = hist.get_history("Market intelligence")
        self.mark_df = hist.to_df(self.mark_intel)

        # Market research
        #self.mark_res = hist.get_history("Market research")
        #self.mark_res_df = hist.to_df(self.mark_res)

        # Open intelligence
        self.open_intel = hist.get_history("Open-source intelligence")
        self.open_df = hist.to_df(self.open_intel)

        # Product intelligence
        self.product_intel = hist.get_history("Product intelligence")
        self.product_df = hist.to_df(self.product_intel)

        self.clean_data()

d = Data()
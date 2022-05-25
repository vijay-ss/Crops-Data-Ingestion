import os
import logging

import pandas as pd
import numpy as np
import openpyxl
import lxml
import re

os.chdir('/opt/airflow/dags')

# enable logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('transform_data.log')
file_handler.setFormatter((formatter))
logger.addHandler((file_handler))


def update_region(row):
    """update null regions based on state code"""
    # https://www.ars.usda.gov/northeast-area/beltsville-md-bhnrc/beltsville-human-nutrition-research-center/docs/regions/
    
    if row['State_Code'] == 2:
        row['EZT_Region_Key'] = 60
        row['Region_Name'] = 'USDA - West'
    if row['State_Code'] == 33:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 35:
        row['EZT_Region_Key'] = 60
        row['Region_Name'] = 'USDA - West'
    if row['State_Code'] == 44:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 50:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 60:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 66:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 69:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 72:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 78:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other' 
        
    return row


# TODO: split this function into separate DAGs
def transform_callable():
    """Wrapper function to be called by Airflow"""

    ## Import Source Data Files ##

    
    url_treated_acres = r"https://raw.githubusercontent.com/vijay-ss/Crops-Data-Ingestion/main/Treated%20Acres.csv"
    url_products = r"https://github.com/vijay-ss/Crops-Data-Ingestion/raw/main/Market%20Size.xlsx"
    url_state_codes = r"https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696"

    df_treated_acres_raw = pd.read_csv(url_treated_acres)
    df_treated_acres_raw.rename(columns={"State_Code": "State_Nm"}, inplace=True)

    logging.info('df_treated_acres_raw record count: {}'.format(len(df_treated_acres_raw)))

    df_products_raw = pd.read_excel(url_products)
    df_products_raw.columns = df_products_raw.columns.str.replace(' ','_')

    # Import treated acres data (can take a minute)
    df_planted_acres_2018 = pd.read_excel("2018_fsa_acres_012819.xlsx", sheet_name="county_data")
    df_planted_acres_2019 = pd.read_excel("2019_fsa_acres_web_010220.xlsx", sheet_name="county_data")
    df_planted_acres_2020 = pd.read_excel("2020_fsa_acres_web_010521.xlsx", sheet_name="county_data")
    df_planted_acres_2018["Crop_Year"] = int("2018")
    df_planted_acres_2019["Crop_Year"] = int("2019")
    df_planted_acres_2020["Crop_Year"] = int("2020")

    logging.info('df_planted_acres_2018 record count: {}'.format(len(df_planted_acres_2018)))
    logging.info('df_planted_acres_2019 record count: {}'.format(len(df_planted_acres_2019)))
    logging.info('df_planted_acres_2020 record count: {}'.format(len(df_planted_acres_2020)))

    # Import usda state codes reference file
    df_ref_state_codes = pd.read_html(url_state_codes, match="State")[0]
    df_ref_state_codes.columns = df_ref_state_codes.columns.str.replace(' ','_')
    df_ref_state_codes.rename(columns={"Name": "State", "Postal_Code": "State_Nm", "FIPS": "State_Code"}, inplace=True)
    df_ref_state_codes.dropna(inplace=True)
    df_ref_state_codes["State_Code"] = df_ref_state_codes["State_Code"].astype('int')
    df_ref_state_codes["State"].replace({'Virgin Islands': 'Virgin Islands of the U.S.',
                                        'Samoa': 'American Samoa'}, inplace=True)

    # TODO: automate this step to pickup all raw dataframes
    raw_tables = [df_products_raw,
            df_treated_acres_raw,
            df_ref_state_codes,
            df_planted_acres_2018,
            df_planted_acres_2019,
            df_planted_acres_2020]

    planted_acres_raw = [df_planted_acres_2018,
                    df_planted_acres_2019,
                    df_planted_acres_2020]

    def clean_data(*args):
        """De-dup source dataframes inplace"""
        for args in raw_tables:
            logger.debug("Row count before: {}".format(len(args)))
            args.drop_duplicates(inplace=True)
            logger.debug("Row count after removing duplicates: {}".format(len(args)))
    
    clean_data()

    # consolidate planted acres into one dataframe
    df_planted_acres_all = pd.concat([df_planted_acres_2018,
                            df_planted_acres_2019,
                            df_planted_acres_2020],
                            ignore_index=True)

    df_planted_acres_all.columns = df_planted_acres_all.columns.str.replace(' ', '_')

    logger.info("Planted acres df record count: {}".format(len(df_planted_acres_all)))


    # add state code to treated acres dataframe
    df_treated_acres_raw = pd.merge(df_treated_acres_raw, df_ref_state_codes,
                          how="left",
                          on=['State_Nm']).rename(columns={"State_x": "State"})


    ## Products Dataframe ##


    # aggregate products table
    df_products_agg = df_products_raw.groupby(["Budget_Year", "Product_Category_Name", "EZT_Region_Key", "Region_Name"]) \
        .agg({"Market_Share": "sum",
        "Total_Dollars": "sum"}).reset_index()

    logger.info("df_products_agg record count: {}".format(len(df_products_agg)))

    # pivot product category to column
    df_products_agg_pv = df_products_agg.pivot(index=df_products_agg.columns.drop(["Product_Category_Name", "Market_Share"]), \
                            columns="Product_Category_Name", \
                            values="Market_Share").rename_axis(columns=None).reset_index()
    df_products_agg_pv.columns = df_products_agg_pv.columns.str.replace(' ','_')

    logger.info("df_products_agg_pv record count: {}".format(len(df_products_agg_pv)))

    # aggregate on the new product category columns
    df_products_agg_2 = df_products_agg_pv.groupby(["Budget_Year", "EZT_Region_Key", "Region_Name"]) \
        .agg({"Total_Dollars": "sum",
        "Ag_Fungicide": "sum",
        "Ag_Growth_Regulator": "sum",
        "Ag_Herbicide": "sum",
        "Ag_Insecticide": "sum",
        "Ag_Nematicide": "sum"}).reset_index()

    # rename Ag columns
    for col in df_products_agg_2.columns:
        if re.match("(Ag\w+)", col):
            df_products_agg_2.rename(columns={col: col + "_Market_Share"}, inplace=True)

    ## Treated Acres Dataframe ##

    # pivot Total_Sum_Product_Amt_Used_In_LB_or_GL by UOM
    df_treated_acres_raw_pv = df_treated_acres_raw.pivot(index=df_treated_acres_raw.columns.drop(["UOM_Code"]), \
                            columns="UOM_Code", \
                            values="Total_Sum_Product_Amt_Used_In_LB_or_GL") \
    .rename(columns={"GL": "Total_Sum_Product_Amt_Used_In_GL", "LB": "Total_Sum_Product_Amt_Used_In_LB"}) \
    .reset_index().rename_axis(None, axis=1)

    logger.info("df_treated_acres_raw_pv record count: {}".format(len(df_treated_acres_raw_pv)))

    df_treated_acres_raw_pv["Total_Sum_Product_Amt_Used_In_GL"] = df_treated_acres_raw_pv["Total_Sum_Product_Amt_Used_In_GL"].replace(np.nan, 0)
    df_treated_acres_raw_pv["Total_Sum_Product_Amt_Used_In_LB"] = df_treated_acres_raw_pv["Total_Sum_Product_Amt_Used_In_LB"].replace(np.nan, 0)

    # TODO: make this a function
    #create product & crop count columns

    # Crops category
    df_treated_acres_raw_pv["Row_Crops_Cnt"] = [1 if a == "Row Crops" else 0 for a in df_treated_acres_raw_pv['Row_vs_Specialty_Crops']]
    df_treated_acres_raw_pv["Specialty_Crops_Cnt"] = [1 if a == "Specialty Crops" else 0 for a in df_treated_acres_raw_pv['Row_vs_Specialty_Crops']]
    df_treated_acres_raw_pv["Other_Crops_Cnt"] = [1 if a == "Other Crops" else 0 for a in df_treated_acres_raw_pv['Row_vs_Specialty_Crops']]
    df_treated_acres_raw_pv["Fallow_Range_Crops_Cnt"] = [1 if a == "Fallow & Range" else 0 for a in df_treated_acres_raw_pv['Row_vs_Specialty_Crops']]

    # Products category
    df_treated_acres_raw_pv["Ag_Fungicide_Product_Cnt"] = [1 if a == "Ag Fungicide" else 0 for a in df_treated_acres_raw_pv['Product_Category_Name']]
    df_treated_acres_raw_pv["Ag_Growth_Regulator_Product_Cnt"] = [1 if a == "Ag Growth Regulator" else 0 for a in df_treated_acres_raw_pv['Product_Category_Name']]
    df_treated_acres_raw_pv["Ag_Herbicide_Product_Cnt"] = [1 if a == "Ag Herbicide" else 0 for a in df_treated_acres_raw_pv['Product_Category_Name']]
    df_treated_acres_raw_pv["Ag_Insecticide_Product_Cnt"] = [1 if a == "Ag Insecticide" else 0 for a in df_treated_acres_raw_pv['Product_Category_Name']]
    df_treated_acres_raw_pv["Ag_Nematicide_Product_Cnt"] = [1 if a == "Ag Nematicide" else 0 for a in df_treated_acres_raw_pv['Product_Category_Name']]

    #Aggregate treated acres table
    df_treated_acres_agg = df_treated_acres_raw_pv.groupby(["Budget_Year", "EZT_Region_Key", "Region_Name"]) \
        .agg({"Row_Crops_Cnt": "sum",
            "Specialty_Crops_Cnt": "sum",
            "Other_Crops_Cnt": "sum",
            "Fallow_Range_Crops_Cnt": "sum",
            "Total_Sum_Area_Treated": "sum",
            "Ag_Fungicide_Product_Cnt": "sum",
            "Ag_Growth_Regulator_Product_Cnt": "sum",
            "Ag_Herbicide_Product_Cnt": "sum",
            "Ag_Insecticide_Product_Cnt": "sum",
            "Ag_Nematicide_Product_Cnt": "sum",
            "Total_Sum_Product_Amt_Used_In_GL": "sum",
            "Total_Sum_Product_Amt_Used_In_LB": "sum"}).reset_index()

    logger.info("df_treated_acres_agg record count: {}".format(len(df_treated_acres_agg)))


    ## Merge Treated Acres and Product Dataframes ##


    df_treated_acres_x_products = pd.merge(df_treated_acres_agg, df_products_agg_2,
        how="left",
        on=["Budget_Year", "EZT_Region_Key"]) \
            [['Budget_Year',
            'EZT_Region_Key',
            'Region_Name_x',
            'Row_Crops_Cnt',
            'Specialty_Crops_Cnt',
            'Other_Crops_Cnt',
            'Fallow_Range_Crops_Cnt',
            'Total_Sum_Area_Treated',
            'Ag_Fungicide_Product_Cnt',
            'Ag_Growth_Regulator_Product_Cnt',
            'Ag_Herbicide_Product_Cnt',
            'Ag_Insecticide_Product_Cnt',
            'Ag_Nematicide_Product_Cnt',
            'Total_Sum_Product_Amt_Used_In_GL',
            'Total_Sum_Product_Amt_Used_In_LB']] \
                .rename(columns={"Region_Name_x": "Region_Name"}) \
                    .reset_index(drop=True)

    logger.info("df_treated_acres_x_products record count: {}".format(len(df_treated_acres_x_products)))


    ## Planted Acres Dataframe ##


    # helper table 
    df_ref_region = df_treated_acres_raw.drop_duplicates(['State_Code', 'State_Nm', 'State', 'EZT_Region_Key', 'Region_Name']) \
        [['State_Code', 'State_Nm', 'State', 'EZT_Region_Key', 'Region_Name']] \
            .reset_index(drop=True)

    # map each region to state code
    # df_state_region_xref = pd.merge(df_ref_state_codes, df_ref_region, how='left', on=['State_Code']) \
    #     .drop_duplicates() \
    #         .rename(columns={"State_x": "State", "State_Nm_x": "State_Nm"}) \
    #             .reset_index(drop=True)

    # df_state_region_xref = df_state_region_xref[['State_Code', 'State', 'State_Nm', 'EZT_Region_Key', 'Region_Name']]


    # df_state_region_xref.apply(update_region, axis=1)

    # join ref region to planted acres dataframe
    df_planted_acres_x_region = pd.merge(df_planted_acres_all, df_ref_region,
        how="left",
        on=['State_Code']).rename(columns={"State_x": "State"})

    logger.info("df_planted_acres_x_region record count: {}".format(len(df_planted_acres_x_region)))

    # aggregate planted acres dataframe
    df_planted_acres_agg = df_planted_acres_x_region.groupby(['Crop_Year', 'EZT_Region_Key', 'Region_Name']) \
        .agg({'Planted_Acres': 'sum',
        'Volunteer_Acres': 'sum',
        'Failed_Acres': 'sum',
        'Prevented_Acres': 'sum',
        'Not_Planted_Acres': 'sum',
        'Planted_and_Failed_Acres': 'sum'}).reset_index()

    logger.info("df_planted_acres_agg record count: {}".format(len(df_planted_acres_agg)))


    ## Create Final Dataset ##

    # join 3 datasets together
    df_final = pd.merge(df_treated_acres_x_products, df_planted_acres_agg,
        how="left",
        left_on=['Budget_Year', 'EZT_Region_Key'],
        right_on=['Crop_Year', 'EZT_Region_Key']) \
            .drop(columns=['Crop_Year', 'Region_Name_y']) \
                .rename(columns={"Region_Name_x": "Region_Name"}) \
                    .drop_duplicates().reset_index(drop=True)

    logger.info("df_final record count: {}".format(len(df_final)))

    df_final.to_excel("Final_Output_Summarized.xlsx", index=False)

    logger.info("df_final written out to xlsx file.")
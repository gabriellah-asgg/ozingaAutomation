import sys
import math
import numpy as np
import pandas as pd
from difflib import *
import os
from pandas import DatetimeIndex
import re
import openpyxl

no_match_flag = "NO_MATCH_FOUND"
main_df_filepath = (r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE '
                    r'3\Envizi_Connector\Main_GWPFactor_Database.xlsx')
inbound_file_start = 'Ozinga-Inbound-Inventory-'
inbound_file_end = '.xlsx'
inbound_inventory_folder_filepath = (
    r'O:\DATA4\Projects\223002\ADMIN\CORRESPONDENCE\Transmittal\Incoming\InboundInventoryReports\RE_ Envizi Connector')
output_filepath_start = (r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE '
                         r'3\Envizi_Connector\S3_Connector_Upload\Ozinga_Scope3_')

distance_filepath = (r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE '
                     r'3\Envizi_Connector\Distances_Database_v2.0.xlsx')

facility_list_filepath = r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2024.03.25_MasterFlatFile\Facility List.xlsx'

material_group_replacement = {"Admixture": "Admixtures", "Other": "Others", "Coarse Aggregate": "CoarseAggregate",
                              "Fine Aggregate": "FineAggregate"}
excel_title_start = r'Ozinga_Scope3_'
industry_averages_file = r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE 3\Envizi_Connector\Reference GWP Factors.xlsx'
actual_gwp_file = r'O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE 3\Envizi_Connector\Actual GWP Factors.xlsx'


def make_unique_name_dict(main_dataframe):
    """takes in main dataframe and makes dictionary to store unique names and their corresponding material"""
    unique_name_material_dict = {}
    for index, row in main_dataframe.iterrows():
        unique_name_material_dict[row['UniqueName']] = row['Material Group']

    return unique_name_material_dict


def get_gwp_replacements(main_dataframe):
    """takes in main dataframe and creates dictionary of material groups and their corresponding missing value
    replacements"""
    gwp_replacements = {'Cement': 922, np.nan: np.nan}
    unique_materials = set(main_dataframe['Material Group'])
    for material in unique_materials:
        if material != 'Cement' and not pd.isnull(material):
            given_material = main_dataframe[main_dataframe['Material Group'] == material]
            if material == 'Others':
                # create nested dictionary for 'Others' based on product
                replacement = get_other_replacements(given_material)
            else:
                # assign overall material average as replacement
                replacement = np.nansum(given_material['Adjusted GWP (per Ozinga UOM)']) / len(
                    given_material['Adjusted GWP (per Ozinga UOM)'])
            gwp_replacements[material] = replacement

    return gwp_replacements


def get_other_replacements(other_dataframe):
    other_dict = {}
    unique_others = set(unique_name[0] for unique_name in other_dataframe['UniqueName'].str.split("_"))

    # group by first part of unique name
    for other in unique_others:
        # filter dataframe by product names in unique names with other material group
        unique_name_df = other_dataframe[other_dataframe['UniqueName'].str.contains(other, na=False)]

        # get average gwp of the given unique product
        avg = np.nansum(unique_name_df['Adjusted GWP (per Ozinga UOM)']) / len(unique_name_df['Adjusted GWP (per '
                                                                                              'Ozinga UOM)'])
        replacement = round(avg, 2)
        other_dict[other] = replacement
    return other_dict


def fix_missing_gwp(main_dataframe, replacement_dictionary):
    """takes in main dataframe and replaces missing gwp value based on gwp dict"""
    unique_materials = set(main_dataframe['Material Group'])
    for material in unique_materials:
        if material == 'Others':
            other_keys = replacement_dictionary['Others'].keys()
            for other_key in other_keys:
                # first replace the source type
                """
                main_dataframe['GWP Source'] = np.where(
                    ((main_dataframe['Material Group'] == material) & (
                        main_dataframe['UniqueName'].str.contains(other_key)) &
                     main_dataframe['Adjusted GWP (per Ozinga UOM)'].isnull()),
                    "Database Average",
                    main_dataframe['GWP Source'])"""

                # then replace the gwp
                main_dataframe['Adjusted GWP (per Ozinga UOM)'] = np.where(
                    ((main_dataframe['Material Group'] == material) & (
                        main_dataframe['UniqueName'].str.contains(other_key)) &
                     main_dataframe['Adjusted GWP (per Ozinga UOM)'].isnull()),
                    replacement_dictionary[material][other_key],
                    main_dataframe['Adjusted GWP (per Ozinga UOM)'])
        else:
            if not pd.isnull(material):
                # first replace the source type
                """
                main_dataframe["GWP Source"] = np.where(
                    ((main_dataframe['Material Group'] == material) & main_dataframe[
                        'Adjusted GWP (per Ozinga UOM)'].isnull()),
                    "Database Average", main_dataframe["GWP Source"])
                    """

                # then replace the gwp
                main_dataframe['Adjusted GWP (per Ozinga UOM)'] = np.where(
                    ((main_dataframe['Material Group'] == material) & main_dataframe[
                        'Adjusted GWP (per Ozinga UOM)'].isnull()),
                    replacement_dictionary[material], main_dataframe['Adjusted GWP (per Ozinga UOM)'])

    return main_dataframe


def populate_facility_and_location(export_dataframe, inbound_inventory_dataframe, facility_dataframe):
    """takes in dataframe to be exported, inbound inventory dataframe and flat file dataframe and returns dataframe
    to be exported with the facility ids and location names populated"""
    facility_ids = []
    location_names = []

    for index, row in inbound_inventory_dataframe.iterrows():
        facility_id = np.nan
        location_name = np.nan
        warehouse_id = [num for num in inbound_inventory_dataframe['delivery_warehouse'][index].split() if
                        num.isnumeric()]
        if warehouse_id:
            # if there is a warehouse id for given inventory, try to find match in facility database
            matching_row = facility_dataframe[facility_dataframe['ID #'] == int(warehouse_id[0])]
            if not matching_row.empty:
                facility_id = matching_row['Facility ID'].iloc[0]
                location_name = matching_row['Location Name'].iloc[0]
        facility_ids.append(facility_id)
        location_names.append(location_name)
    export_dataframe['Facility ID'] = facility_ids
    export_dataframe['Location Name'] = location_names
    return export_dataframe


def check_date(inbound_row, product_matches):
    """
    :param inbound_row: row to check date for
    :param product_matches: rows that matched the inbound row
    :return: adjusted gwp of closest date, whether gwp is expired or not
    """
    date = inbound_row['ticket_date']

    expired = False
    diff = sys.maxsize
    adjusted_gwp = np.nan

    for index, row in product_matches.iterrows():
        effective_from_date = row['Effective From']
        effective_to_date = row['Effective To']

        if effective_from_date.day is np.nan or effective_to_date is np.nan:
            expired = np.nan
            adjusted_gwp = row['Adjusted GWP (per Ozinga UOM)']
            continue

        # if date is past the effective to date, set expired to be true
        if date > effective_to_date:
            expired = True
        else:
            expired = False

        # look for date closest to date of inbound inventory row
        curr_diff = date - effective_to_date
        day_diff = abs(curr_diff.days)
        if day_diff < diff:
            # if date is closest to date, adjust gwp
            diff = day_diff
            adjusted_gwp = row['Adjusted GWP (per Ozinga UOM)']

        if effective_from_date <= date <= effective_to_date:
            adjusted_gwp = row['Adjusted GWP (per Ozinga UOM)']
            break
    return adjusted_gwp, expired


def populate_gwp_and_material(main_replaced_dataframe, export_dataframe, unique_name_to_mat_dict,
                              close_match_dictionary,
                              inbound_unique_names, inbound_inventory_dataframe,
                              gwp_replace_dict):
    """ takes in main database, database to be exported, and unique name dictionary. Iterates over each unique name
    in inbound inventory and finds the corresponding gwp and material from the gwp database. If no match can be
    found, it will try to find the closest match. returns dataframe to be exported with populated gwp and material
    columns"""
    new_material_col = []
    new_adjusted_gwp_col = []
    expired_col = []

    for index, row in inbound_inventory_dataframe.iterrows():

        name = row['unique_name']

        # look for close match
        close_match = close_match_dictionary[name]

        material_name = no_match_flag
        adjusted_gwp = np.nan
        expired = np.nan

        # if close match to name, add material group and average adjusted gwp of that material
        if not pd.isnull(close_match):
            # find material group of closest match
            material_name = unique_name_to_mat_dict.get(close_match, np.nan)

            # if name is an exact match, replace with exact same GWP
            if name == close_match:
                adjusted_gwp_matches = main_replaced_dataframe[main_replaced_dataframe['UniqueName'] == close_match]
                adjusted_gwp, expired = check_date(row, adjusted_gwp_matches)

            # if match is not exact match, replace with average GWP for given material
            else:
                # if Others have to navigate nested dictionary
                if material_name == 'Others':
                    adjusted_gwp = round(gwp_replace_dict['Others'].get(close_match.split()[0], np.nan), 2)
                # else will replace with average
                else:
                    adjusted_gwp = round(gwp_replace_dict.get(material_name, np.nan), 2)

        # add material group to new column
        new_material_col.append(material_name)

        # add adjusted gwp to new column
        new_adjusted_gwp_col.append(adjusted_gwp)

        expired_col.append(expired)

    # populate material column
    export_dataframe['Material Group'] = new_material_col

    # populate adjusted gwp
    export_dataframe['Adjusted GWP (per Ozinga UOM)'] = new_adjusted_gwp_col

    # add temporary expiration column
    export_dataframe['Expired'] = expired_col

    return export_dataframe


def process_unique_names(inbound_inventory_df, unique_names):
    """function that takes in inbound inventory dataframe and returns unique names column; adds unique_name column to
    inbound inventory; returns close matches dictionary and inbound inventory with unique names"""
    unique_names_column = []
    close_matches_dict = {}
    for index, row in inbound_inventory_df.iterrows():
        # create unique name based on product descrip, material supplier & material site
        unique_name = str(inbound_inventory_df['product_description'].iloc[index]) + "_" + str(
            inbound_inventory_df['material_supplier'].iloc[index])
        material_site = inbound_inventory_df['material_site'].iloc[index]
        if not pd.isna(material_site):
            unique_name += ("_" + str(material_site))
        else:
            unique_name += "_null"

        # add unique name to column
        unique_names_column.append(unique_name)

        # find close match from main database and add to dictionary
        if unique_name not in close_matches_dict:

            # find the closest match to the unique name
            close_matches = get_close_matches(unique_name, list(unique_names.keys()))

            # if a close match is found, add it to dictionary of unique names and closest matches
            if close_matches:
                close_matches_dict[unique_name] = close_matches[0]
                # add it to a list to print out
            else:
                close_matches_dict[unique_name] = np.nan
    inbound_inventory_df["unique_name"] = unique_names_column
    return unique_names_column, close_matches_dict, inbound_inventory_df


def filter_inbound_inventory(inbound_inventory_df):
    """takes in inbound inventory dataframe and filters out unwanted rows, returns edited inbound inventory dataframe"""
    inbound_inventory_df = inbound_inventory_df[inbound_inventory_df['delivery_warehouse'].str.contains('SF|RMX')]
    inbound_inventory_df = inbound_inventory_df[~inbound_inventory_df['unit_of_measure'].str.contains('EA')]
    inbound_inventory_df = inbound_inventory_df[~inbound_inventory_df['product_description'].str.contains('DIESEL')]
    inbound_inventory_df.reset_index(drop=True, inplace=True)
    return inbound_inventory_df


def filter_facility_list(facility_dataframe):
    """takes in facility dataframe and filters out unwanted rows,returns edited facility list dataframe """
    facility_dataframe = facility_dataframe[facility_dataframe['Facility Type'].str.contains('Ready Mix Plant')]
    facility_dataframe.reset_index(drop=True, inplace=True)
    return facility_dataframe


def extract_output_filepath(inbound_inventory_filepath):
    """returns string of month and year from the given filepath"""
    file_end = inbound_inventory_filepath.split(inbound_file_start)
    file_end = re.split(r'[.\-]', file_end[1])
    output_end = ""
    for n in file_end:
        output_end += str(n)
        if str(n) != 'xlsx':
            output_end += "."
    return output_end


def populate_distances(inbound_inventory_dataframe, distance_dataframe, export_dataframe):
    """takes in inbound inventory dataframe, distance dataframe, and dataframe to be exported and populates distance
    columns for exported data"""
    truck_dists = []
    rail_dists = []
    ocean_dists = []
    barge_dists = []
    not_in_dist_db = inbound_inventory_dataframe.copy()

    # preprocess distance database
    processed_supplier_to_delivery_warehouse = [s.replace(" ", "").replace("\n", "").replace("_x000D_", "").lower() for s in distance_dataframe['supplier_to_delivery_warehouse']]
    distance_dataframe['processed_supplier_to_delivery_warehouse'] = processed_supplier_to_delivery_warehouse

    # populate distances from distance dataframe
    for index, row in inbound_inventory_dataframe.iterrows():
        truck_dist = np.nan
        rail_dist = np.nan
        ocean_dist = np.nan
        barge_dist = np.nan

        delivery_warehouse = inbound_inventory_dataframe['delivery_warehouse'][index]
        address = inbound_inventory_dataframe['material_site_address'][index]
        if (not pd.isnull(address)) and (not pd.isnull(delivery_warehouse)):
            delivery_warehouse = delivery_warehouse.split()[0].replace(" ", "").replace("\n", "")
            address = address.replace(" ", "").replace("\n", "")
            supplier_to_delivery_warehouse = address.lower() + "_" + delivery_warehouse.lower()
            matching_row = distance_dataframe[
                distance_dataframe['processed_supplier_to_delivery_warehouse'] == supplier_to_delivery_warehouse]

            if not matching_row.empty:
                truck_dist = matching_row['Truck (miles)'].iloc[0]
                rail_dist = matching_row['Rail (miles)'].iloc[0]
                ocean_dist = matching_row['Ocean (miles)'].iloc[0]
                barge_dist = matching_row['Barge (miles)'].iloc[0]
                not_in_dist_db.drop(index=index, axis=0, inplace=True)

        truck_dists.append(truck_dist)
        rail_dists.append(rail_dist)
        ocean_dists.append(ocean_dist)
        barge_dists.append(barge_dist)

    export_dataframe['Truck (miles)'] = truck_dists
    export_dataframe['Train (miles)'] = rail_dists
    export_dataframe['Ocean (miles)'] = ocean_dists
    export_dataframe['Barge (miles)'] = barge_dists

    return export_dataframe, not_in_dist_db


def find_most_recent_file(inbound_inventory_folder_path, inbound_start, inbound_end):
    """takes in filepath of folder, start of inbound filepath, and end of filepath and returns the filepath to the
    most recent inbound inventory"""
    file_list = os.listdir(inbound_inventory_folder_path)

    # collect all files that begin with start convention
    matching_files = [filename for filename in file_list if filename.startswith(inbound_file_start)]

    # preprocess filenames to extract dates
    match_file_dates = [filedate.replace(inbound_start, "").replace(inbound_end, "") for filedate in
                        matching_files]
    dates = np.array([DatetimeIndex([date]) for date in match_file_dates])

    # find index of max date in list of dates
    max_index = dates.argmax()

    # get file with max date
    inbound_inventory_filepath = inbound_inventory_folder_path + '\\' + matching_files[max_index]
    return inbound_inventory_filepath


def generate_report(main_database, export_database, inbound_database, report_dataframes):
    """takes in main dataframe, dataframe to export, and the inbound inventory dataframe. Uses these dataframes to
    generate a report of matching percentages and important missing values"""

    # check for names in inbound inventory that don't exist in main database
    unique_name_not_found = export_database[~export_database['unique_name'].isin(main_database['UniqueName'])]

    # check for names in inbound inventory that don't have adjusted gwp filled
    nan_gwp = export_database[export_database['Adjusted GWP (per Ozinga UOM)'].isna()]

    # check for names in inbound inventory that don't have location name in facility list
    nan_loc_id = export_database[export_database['Location Name'].isna()]
    nan_loc_warehouses = inbound_database[inbound_database['unique_name'].isin(nan_loc_id['unique_name'])][
        'delivery_warehouse']
    nan_loc_id.insert(1, 'delivery_warehouse', nan_loc_warehouses)

    # check for expired
    expired_gwp = export_database[export_database['Expired'] == True]

    nan_distances = report_dataframes["Locations not in Distance DB"]
    nan_distances = nan_distances[['unique_name','material_site_address', 'delivery_warehouse', 'delivery_warehouse_address']]

    # remove unique name duplicates from dataframes
    unique_name_not_found = unique_name_not_found.drop_duplicates(subset=['unique_name'])
    nan_distances = nan_distances.drop_duplicates(subset=['unique_name'])
    nan_loc_id = nan_loc_id.drop_duplicates(subset=['unique_name'])
    nan_gwp = nan_gwp.drop_duplicates(subset=['unique_name'])
    expired_gwp = expired_gwp.drop_duplicates(subset=['unique_name'])

    # calculate match percentages
    match_percentages = {}

    # find percentages for gwp, truck, location
    gwp_match_percentage = round(((len(export_database) - len(nan_gwp)) / len(export_database)) * 100, 2)
    unique_name_percentage = round(((len(export_database) - len(unique_name_not_found)) / len(export_database)) * 100,
                                   2)
    nan_truck = export_database[export_database['Truck (miles)'].isna()]
    truck_match_percentage = round(((len(export_database) - len(nan_truck)) / len(export_database)) * 100, 2)
    location_percentage = round(((len(export_database) - len(nan_loc_id)) / len(export_database)) * 100, 2)
    expired_gwp_percentage = round(((len(expired_gwp)) / len(export_database)) * 100, 2)

    # add match percentages to dictionary
    match_percentages["% GWPs Filled In"] = str(gwp_match_percentage) + "%"
    match_percentages["Matching Material Names in GWP DB"] = str(unique_name_percentage) + "%"
    match_percentages["Matching Truck Distances"] = str(truck_match_percentage) + "%"
    match_percentages["Matching Locations"] = str(location_percentage) + "%"
    match_percentages["Expired GWPs"] = str(expired_gwp_percentage) + "%"
    percentage_dataframe = pd.DataFrame(data=match_percentages, index=[0])

    # add created dataframes to the list of dataframes to be returned
    report_dataframes["Match Percentages"] = percentage_dataframe
    report_dataframes["UniqueNames not in GWP DB"] = unique_name_not_found
    report_dataframes["Locations not in Distance DB"] = nan_distances
    report_dataframes["Materials Missing GWP Factor"] = nan_gwp
    report_dataframes["Locations not in FacilityList"] = nan_loc_id
    report_dataframes["Expired GWPs"] = expired_gwp
    return report_dataframes


def process_inbound_inventory(main_dataframe, facility_dataframe, distance_dataframe, inbound_filepath,
                              unique_name_dictionary):
    inbound_inventory = pd.read_excel(inbound_filepath)
    inbound_inventory = filter_inbound_inventory(inbound_inventory)

    # make new dataframe to export
    export_df_columns = {'unique_name': np.zeros(len(inbound_inventory)),
                         'Material Group': np.zeros(len(inbound_inventory)),
                         'Location Name': np.zeros(len(inbound_inventory)),
                         'Facility ID': np.zeros(len(inbound_inventory)),
                         'Sum of quantity': inbound_inventory['quantity'],
                         'unit_of_measure': inbound_inventory['unit_of_measure'],
                         'Adjusted GWP (per Ozinga UOM)': np.zeros(len(inbound_inventory)),
                         'ticket_date': inbound_inventory['ticket_date'],
                         'Truck (miles)': np.zeros(len(inbound_inventory)),
                         'Train (miles)': np.zeros(len(inbound_inventory)),
                         'Ocean (miles)': np.zeros(len(inbound_inventory)),
                         'Barge (miles)': np.zeros(len(inbound_inventory))}
    export_df = pd.DataFrame(data=export_df_columns)

    #  unique names from inbound inventory
    unique_names_col, close_match_dict, inbound_inventory = process_unique_names(inbound_inventory,
                                                                                 unique_name_dictionary)
    export_df['unique_name'] = unique_names_col

    report_dataframes = {}

    # replace missing GWP values in main dataframe
    main_replaced_df = fix_missing_gwp(main_dataframe, replacement_dict)

    # populate dataframe to export
    export_df = populate_gwp_and_material(main_replaced_df, export_df, unique_name_dictionary, close_match_dict,
                                          unique_names_col, inbound_inventory, replacement_dict)
    export_df = populate_facility_and_location(export_df, inbound_inventory, facility_dataframe)
    export_df, nan_distances = populate_distances(inbound_inventory, distance_dataframe, export_df)
    report_dataframes["Match Percentages"] = []
    report_dataframes["Locations not in Distance DB"] = nan_distances

    # remove rows where no match was found
    export_df.dropna(subset=['Material Group'], inplace=True)
    export_df = export_df[~export_df['Material Group'].str.contains(no_match_flag)]

    #export_df['GWP Source'] = main_replaced_df['GWP Source']
    # generate report of nan / missing values
    report_data = generate_report(main_dataframe, export_df, inbound_inventory, report_dataframes)

    # drop the expiration column
    export_df.drop(columns='Expired', inplace=True)

    # fill na values in export dataframe
    export_df.fillna(0, inplace=True)

    # extract month & year of file
    output_path_ending = extract_output_filepath(inbound_filepath)

    # create output filepath name
    output_filepath = output_filepath_start + output_path_ending
    sheet_name = excel_title_start + (output_path_ending.replace(".xlsx", ""))

    # export excel to filepath
    try:
        writer = pd.ExcelWriter(output_filepath)
        export_df.to_excel(writer, sheet_name=sheet_name, index=False)
        writer.close()
        print("\nFile output to: \n" + output_filepath)
    except PermissionError:
        print("\nCannot write to file, try closing " + output_filepath + " and rerunning")

    # write report to Excel file
    report_filepath = output_filepath.replace("xlsx", "Report.xlsx")

    try:
        writer = pd.ExcelWriter(report_filepath)
        for key in report_data.keys():
            dataframe = report_data[key]
            dataframe.to_excel(writer, index=False, sheet_name=key)
        writer.close()
        print("\nReport File output to: \n" + report_filepath)
    except PermissionError:
        print("\nCannot write to file, try closing " + report_filepath + " and rerunning")


# load in main database
main_df = pd.read_excel(main_df_filepath)
main_df.rename(columns=lambda column: column.strip(), inplace=True)
main_df.replace(to_replace={"Material Group": material_group_replacement}, inplace=True)

# create a dictionary of replacements for material group GWPs
replacement_dict = get_gwp_replacements(main_df)

# import facility list
facility_df = pd.read_excel(facility_list_filepath)
facility_df = filter_facility_list(facility_df)

# import distance dataframe
distance_df = pd.read_excel(distance_filepath, sheet_name=r'Sheet1')

# make dictionary of unique names based on main database
unique_name_dict = make_unique_name_dict(main_df)

user_input = input("Enter filepaths separated by commas to process specific files or hit enter to process most recent "
                   "file:\n")

# find most recent inbound inventory file
filepath_inbound_inventory = [find_most_recent_file(inbound_inventory_folder_filepath, inbound_file_start,
                                                    inbound_file_end)]

# if there is input from the user, use that as filepath
if user_input:
    filepath_inbound_inventory = user_input.split(",")

for inbound_file in filepath_inbound_inventory:
    print("\nProcessing: " + inbound_file + " ...")

    # strip inbound filepath
    inbound_file = inbound_file.strip()
    inbound_file = inbound_file.strip('"')

    # read in inbound inventory
    try:
        process_inbound_inventory(main_df, facility_df, distance_df, inbound_file, unique_name_dict)
    except FileNotFoundError:
        # if file can not be found, print out error message
        print("\nError, could not find file input. Check the filepath and try again")

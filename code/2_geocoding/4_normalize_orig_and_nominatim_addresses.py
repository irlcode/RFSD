# SDK Pullenti Address (client version), version 4.32, November 2025.
# Copyright (c) 2013-2025, Pullenti. All rights reserved.
# Non-Commercial Freeware and Commercial Software.
# This class is generated using the converter Unisharping (www.unisharping.ru) from Pullenti C# project.
# The latest version of the code is available on the site www.pullenti.ru

import os, sys
import typing

# Add path to Pullenti Client
sys.path.append('./code/2_geocoding/pullenti/AddressMinPython')

from pullenti.unisharp.Utils import Utils
from pullenti.address.AddrLevel import AddrLevel
from pullenti.address.ProcessTextParams import ProcessTextParams
from pullenti.address.AddressService import AddressService
from pullenti.address.internal.ServerHelper import ServerHelper
from pullenti.address.SearchParams import SearchParams
from pullenti.address.SearchResult import SearchResult
from pullenti.address.GarObject import GarObject
from pullenti.address.GarParam import GarParam
from pullenti.address.GpsObject import GpsObject
from pullenti.address.HouseAttributes import HouseAttributes

import pandas as pd
import glob
from pathlib import Path
from tqdm.auto import tqdm
import time

# Connect to the running .NET server initiated above 
server_url = "http://localhost:2222"
AddressService.set_server_connection(server_url)

# Set up parameters (see https://garfias.ru/assets/docs/AddressPythonDoc/topic_address_python_pullenti.address.ProcessTextParams.html#DefaultRegions)
pars = ProcessTextParams()
# Assume that addresses have no flats by default
pars.no_flats = True
# With stemming of addresses
pars.classic_address_model_records = False

# Map GAR Param IDs to column names for cleaner code
# See: https://garfias.ru/assets/docs/AddressPythonDoc/index.html
GAR_PARAM_MAP = {
    100: 'latlon_centroid', # Space-delimited latitude and longitude from Cadastral map
    3:   'postalindex', # Postal index
    4:   'okato',
    5:   'oktmo',
    6:   'cadaster_number', # Cadastral number
    104: 'purpose', # Building purpose
    105: 'floors',
    106: 'year_built' # Year built
}

EMPTY_RESULT_TEMPLATE = {
    'ID': '', 'address': '', 'coef': 0, 'level': '', 'fias_count': 0, 'fias_level': '', 'fias': '',
    'address_cleaned': '', 
    **{k: '' for k in GAR_PARAM_MAP.values()}
}

def process_address_row(row, id_field, address_field, pars_arg):
    """
    Processes a single row of the address DataFrame, returning a list of 
    parsed address dictionaries in long form
    """
    ID = row[id_field]
    address = row[address_field]
    
    res = EMPTY_RESULT_TEMPLATE.copy()
    res[id_field] = ID
    res[address_field] = address

    # Ensure the address is a non-empty string before processing
    if not isinstance(address, str) or not address.strip():
        # Return a single entry with empty data if the input is invalid
        return [res]

    # Process the address text using Pullenti
    try:
        saddr = AddressService.process_single_address_text(address, pars_arg)
    except Exception as e:
        return [res]

    if saddr is None or not saddr.items:
        return [res]
    else:

        # This list will hold all results for this single input row
        results_for_this_row = []

        # If addresses are parsed, loop through each item and create a record
        for item in saddr.items: 

            # Base dictionary for this item
            row_data = res.copy()
            row_data['coef'] = saddr.coef
            row_data['level'] = Utils.enumToString(item.level)
            row_data['address_cleaned'] = str(item)

            # Initialize GAR fields as empty
            gar_data = {
                'fias_count': 0, 'fias_level': '', 'fias': '',
                **{k: '' for k in GAR_PARAM_MAP.values()}
            }

            if len(item.gars) > 0:

                # One object can resolve to multiple FIASes
                gar_data['fias_count'] = len(item.gars)

                # We take the first FIAS in case multiple are detected
                garobj = item.gars[0]
                gar_data['fias_level'] = str(garobj.level)
                gar_data['fias'] = str(garobj.guid)

                # Extract params using the map
                for param_id, col_name in GAR_PARAM_MAP.items():
                    val = garobj.get_param_value(param_id)
                    gar_data[col_name] = str(val) if val is not None else None

                # Merge and add to results
                row_data.update(gar_data)

            # Append to row
            results_for_this_row.append(row_data)

        return results_for_this_row

    # Avoid race condition
    time.sleep(0.001)

# Init progressbar
tqdm.pandas(desc="Processing Addresses")

# Import data
addresses_data_frame = pd.read_csv("YOUR_DIR/geocoded_addresses_nominatim.csv", usecols = ['addr_id', 'addr_string', 'addr_string.nominatim'])

# Process and store in a list
addresses_orig_processed = addresses_data_frame.progress_apply(
    process_address_row, 
    axis=1,
    id_field="addr_id",
    address_field="addr_string",
    pars_arg=pars
)
addresses_nominatim_processed = addresses_data_frame.progress_apply(
    process_address_row, 
    axis=1,
    id_field="addr_id",
    address_field="addr_string.nominatim",
    pars_arg=pars
)

# To a DataFrame
addresses_orig_processed = pd.DataFrame(addresses_orig_processed.explode().tolist())
addresses_nominatim_processed = pd.DataFrame(addresses_nominatim_processed.explode().tolist())

# Reset index
addresses_orig_processed.reset_index(drop=True, inplace=True)
addresses_nominatim_processed.reset_index(drop=True, inplace=True)

# Save
addresses_orig_processed.to_csv("YOUR_DIR/pullenti_addr_orig.csv")
addresses_nominatim_processed.to_csv("YOUR_DIR/pullenti_addr_nominatim.csv")

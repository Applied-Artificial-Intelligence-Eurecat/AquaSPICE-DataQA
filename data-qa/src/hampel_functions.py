################################################################################### Imports
import numpy as np
import pandas as pd
from hampel import hampel
import math

import auxiliar_functions as aux_func
import context_broker_client_utils as aquaspice_utils

def outlier_function_hampel_filter(config, station_id, variable, data, hampel_filter_measurements):
    """
    Function to implement Hampel Filter method
    """
    #global hampel_filter_measurements

    short_id = station_id.split(":")[4]
    data = float(data)

    data_list = hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"][variable].values
    data_observations = np.copy(data_list)

    # Append data point and get its index
    data_list = list(data_list)[-(config["property_sliding_window"][variable]):]
    aux_func.logMessage(f"Len data list (start): {len(data_list)}", kind = "debug")

    # Append sample of interest
    data_list.append(data)

    # Get index of the value
    index_data_point = len(data_list) - 1 if len(data_list) >= 2 else 0

    # Look at the lenght of available data, and adjust the sliding window size according to that
    hampel_window_size = math.floor((len(data_list) / 2))

    if hampel_window_size > config["property_sliding_window"][variable]:
        hampel_window_size = config["property_sliding_window"][variable]

    # Debug
    aux_func.logMessage(f"Len data list (end): {len(data_list)}", kind = "debug")
    if len(data_list) >= 2:
        aux_func.logMessage(f"--> Debug: min({np.min(data_list)}), max({np.max(data_list)}), mean({np.mean(data_list)}) current value: {data}, sliding_window: {hampel_window_size}", kind = "debug")

    # If the sliding window is at least 100
    if hampel_window_size >= 100:
        # Run outlier detection, must convert to pd Series
        detected_outliers = hampel(pd.Series(data_list), window_size = hampel_window_size,
                                   n = config["hampel_filter_threshold"],
                                   imputation = False)

        aux_func.logMessage(f"Total outliers in detected_outliers: {len(detected_outliers)}")

        del data_list

        # Debug
        # Check if the data point is an outlier or not
        if index_data_point in detected_outliers:
            aux_func.logMessage("---> Hampel decided Outlier, waiting for IQR confirmation.", kind = "warning")
            # IQR Failsafe
            if aux_func.iqr_method(data_observations, data, config["iqr_threshold"]["default"]) == True:
                aux_func.logMessage(f"--> Outlier ({data}) detected by hampel filter.====================================================", kind = "warning")
                return True
            else:
                # Second check
                if aux_func.iqr_method(data_observations, data, config["iqr_threshold"]["failsafe"]) == True:
                    aux_func.logMessage(f"--> IQR ({config['iqr_threshold']['failsafe']}) considered {data} an outlier.", kind = "warning")
                    return True
                else:
                    aux_func.logMessage(f"--> Hampel filter did not consider {data} an outlier. (After checking IQR)")
                    return False
        else:
            aux_func.logMessage(f"--> Hampel filter did not consider {data} an outlier.")
            return False
    else:
        return False

def hampel_filter_module(config, station_id, analysis, hampel_filter_measurements, anomaly_status, start_from_0 = False):
    '''
    Module responsible for executing the initial in-memory population of data for the hampel filter algorithm to work
    # start_from_0, True = Do not get historic data, False = Get historic data
    '''
    aux_func.logMessage('--> Started hampel_filter module.')

    #global hampel_filter_measurements, anomaly_status

    query_points = config["query_points"]
    historic_data = None

    # Get shorter measurement station id (for dictionary naming)
    short_id = station_id.split(":")[4]

    hampel_filter_measurements["hampel_filter_" + str(short_id)] = {}

    if station_id in anomaly_status:
        pass
    else:
        anomaly_status[station_id] = {}

    if start_from_0 == True:
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                hampel_filter_measurements["hampel_filter_" + str(short_id)][variable] = {
                    "data": [],
                    "num_data": None,
                    "date_updated" : aux_func.get_datetime_now()
                }

                # Anomaly control
                if variable in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][variable] = {}
                    anomaly_status[station_id][variable]["startDateOfOngoingAnomaly"] = None

    elif start_from_0 == False:
        # Get historic data
        historic_data = aquaspice_utils.query_historical_data_lastN(f'urn:ngsi-ld:AquaSpice:{entityType}:{short_id}', query_points)

        # Iterate over properties defined on config file
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                hampel_filter_measurements["hampel_filter_" + str(short_id)][variable] = {
                    "data": pd.DataFrame(columns = ["date", str(variable)]),
                    "num_data": None,
                    "date_updated" : aux_func.get_datetime_now()
                }

                # Anomaly control
                if variable in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][variable] = {}
                    anomaly_status[station_id][variable]["startDateOfOngoingAnomaly"] = None

                # Populate dicts
                if historic_data != None:
                    aux_func.logMessage("--> Creating dataframes based on historical data (Hampel).")

                    hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"]["date"] = pd.to_datetime(historic_data["index"])
                    hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"][variable] = next(e for e in historic_data["attributes"] if e["attrName"] == variable)["values"]

                    # Get the number of available data samples
                    hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["num_data"] = len(hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"])

                    aux_func.logMessage(hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"].head())

        aux_func.logMessage(f"---> Finished creating variables for urn = {station_id}")

        # Free memory
        del historic_data

    return hampel_filter_measurements, anomaly_status
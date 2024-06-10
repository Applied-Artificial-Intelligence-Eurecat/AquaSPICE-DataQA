################################################################################### Imports
import numpy as np
import pandas as pd

import context_broker_client_utils as aquaspice_utils
import auxiliar_functions as aux_func

def outlier_function_watercps(config, station_id : str, variable : str, data, watercps_measurements, debug = False):
    """
    series = list with a few samples (including the newest received)
    variable = str
    config_dict = dict from data_qa_params.json with key "watercps_error_flagging"

    returns outlier True/False and Reason (None if not outlier)
    """

    # Get short id
    short_id = station_id.split(":")[4]

    is_outlier = "No"
    reason = "None"

    if watercps_measurements["watercps_" + str(short_id)][variable]["num_data"] >= 4:

        # Get latest 3 readings
        series = list(watercps_measurements["watercps_" + str(short_id)][variable]["data"][variable].values[-3:])
        # Add current reading
        series = np.hstack((series, data))

        # Calculate mean of 1-hour.
        mean_value = np.mean(series)
        delta_value = np.abs(series[3] - series[2])

        if debug:
            aux_func.logMessage(f"---O Debug watercps_method {variable}:", kind = "debug")
            aux_func.logMessage(f"Mean value: {mean_value}", kind = "debug")
            aux_func.logMessage(f"Delta value: {delta_value}", kind = "debug")

        # Check each case of the threshold
        if mean_value > config["harbour_docks"][variable]["max_value"]:
            if debug:
                aux_func.logMessage("- watercps max value triggered", kind = "debug")
            is_outlier = "Yes"
            reason = "reason_max"
        elif mean_value < config["harbour_docks"][variable]["min_value"]:
            if debug:
                aux_func.logMessage("- watercps min value triggered", kind = "debug")
            is_outlier = "Yes"
            reason = "reason_min"
        elif delta_value > config["harbour_docks"][variable]["delta_value"]:
            if debug:
                aux_func.logMessage("- watercps delta value triggered", kind = "debug")
            is_outlier = "Yes"
            reason = "reason_delta"

        return is_outlier, reason

    else:
        aux_func.logMessage("Insufficient data to compute watercps method (need 4).")
        return is_outlier, reason


def watercps_module(config, station_id, analysis, watercps_measurements, anomaly_status, start_from_0 = False):
    '''
    Process responsible for doing the initial population of in-memory dicts. (WaterCPS threshold)
    # start_from_0, True = Do not get historic data, False = Get historic data
    '''
    #global watercps_measurements, anomaly_status
    aux_func.logMessage('--> Started watercps module.')

    query_points = config["query_points"]
    historic_data = None

    # Get shorter measurement station id (for dictionary naming)
    short_id = station_id.split(":")[4]

    # Expand dicts
    watercps_measurements["watercps_" + str(short_id)] = {}

    if station_id in anomaly_status:
        pass
    else:
        anomaly_status[station_id] = {}

    # Do the same, but don't populate the dicts
    if start_from_0 == True:
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                watercps_measurements["watercps_" + str(short_id)][variable] = {
                    "data": pd.DataFrame(columns=["date", str(variable)]),
                    "num_data": None,
                    "startDateOfOngoingAnomaly": None,
                }

                # Anomaly control
                if variable in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][variable] = {}
                    anomaly_status[station_id][variable]["startDateOfOngoingAnomaly"] = None

        aux_func.logMessage(f"---> Finished creating watercps variables for urn = {station_id}")

    elif start_from_0 == False:
        historic_data = aquaspice_utils.query_historical_data_lastN(f'urn:ngsi-ld:AquaSpice:{entityType}:{short_id}', query_points)

        # Iterate over defined properties
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                watercps_measurements["watercps_" + str(short_id)][variable] = {
                    "data": pd.DataFrame(columns=["date", str(variable)]),
                    "num_data": None,
                    "startDateOfOngoingAnomaly": None,
                }

                # Anomaly control
                if variable in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][variable] = {}
                    anomaly_status[station_id][variable]["startDateOfOngoingAnomaly"] = None

                # Populate dict with in-memory data
                if historic_data is not None:
                    aux_func.logMessage("--> Creating dataframes based on historical data (WaterCPS).")

                    watercps_measurements["watercps_" + str(short_id)][variable]["data"]["date"] = pd.to_datetime(historic_data["index"])
                    watercps_measurements["watercps_" + str(short_id)][variable]["data"][variable] = next(e for e in historic_data["attributes"] if e["attrName"] == variable)["values"]

        aux_func.logMessage(f"---> Finished creating watercps variables for urn = {station_id}")

        # Free memory
        del historic_data

    return watercps_measurements, anomaly_status
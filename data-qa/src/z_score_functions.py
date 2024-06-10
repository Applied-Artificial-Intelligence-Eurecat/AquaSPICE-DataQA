################################################################################### Imports
import numpy as np
import pandas as pd

import auxiliar_functions as aux_func
import context_broker_client_utils as aquaspice_utils

def outlier_function_z_score(config, station_id, variable, threshold, data, entities_data):
    """
    Function to implement z-score method
    """
    data = float(data)

    short_id = station_id.split(":")[4]
    aux_func.logMessage(f"Len data list: {len(entities_data['z_score_measurement_' + str(short_id)][variable]['data'])}")
    aux_func.logMessage(
        f"--> Debug: mean({entities_data['z_score_measurement_' + str(short_id)][variable]['mean']}), std: {entities_data['z_score_measurement_' + str(short_id)][variable]['std']} current value: {data}", kind = "debug")

    if len(entities_data['z_score_measurement_' + str(short_id)][variable]['data']) >= 200:
        mean = entities_data["z_score_measurement_" + str(short_id)][variable]["mean"]
        std = entities_data["z_score_measurement_" + str(short_id)][variable]["std"]

        z = np.abs((data - mean) / std)
        aux_func.logMessage(f"Z value: {z}")

        # If it is an outlier return True, otherwise False
        outlier = True if z >= threshold else False

        # Debug
        if outlier == False:
            aux_func.logMessage(f"--> Z-Score did not consider {data} an outlier.")
        elif outlier == True:
            if aux_func.iqr_method(entities_data['z_score_measurement_' + str(short_id)][variable]['data'].values, data,
                                   config["iqr_threshold"]["default"]):
                aux_func.logMessage(
                    f"--> Z-score considered {data} an outlier ====================================================", kind = "warning")
                outlier = True
            else:
                # Second check
                if aux_func.iqr_method(entities_data['z_score_measurement_' + str(short_id)][variable]['data'].values,
                                       data, config["iqr_threshold"]["failsafe"]):
                    aux_func.logMessage(
                        f"--> IQR ({config['iqr_threshold']['failsafe']}) considered {data} an outlier.", kind = "warning")
                    outlier = True
                else:
                    aux_func.logMessage(f"--> Z-score did not consider {data} and outlier (after checking all IQRs).")
                    outlier = False

        return outlier
    else:
        return False


def z_score_module(config, station_id, analysis, entities_data, anomaly_status, start_from_0 = False):
    '''
    Process responsible for doing the initial population of in-memory dicts.
    # start_from_0, True = Do not get historic data, False = Get historic data
    '''
    #global entities_data, anomaly_status
    aux_func.logMessage('--> Started z-score module.')

    query_points = config["query_points"]
    historic_data = None

    # Get shorter measurement station id (for dictionary naming)
    short_id = station_id.split(":")[4]

    # Expand dicts
    entities_data["z_score_measurement_" + str(short_id)] = {}

    if station_id in anomaly_status:
        pass
    else:
        anomaly_status[station_id] = {}

    # Do the same, but don't populate the dicts
    if start_from_0 == True:
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                entities_data["z_score_measurement_" + str(short_id)][variable] = {
                    "data": pd.DataFrame(columns=["date", str(variable)]),
                    "mean": None,
                    "std": None,
                    "startDateOfOngoingAnomaly": None,
                }

                # Anomaly control
                if variable in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][variable] = {}
                    anomaly_status[station_id][variable]["startDateOfOngoingAnomaly"] = None

    elif start_from_0 == False:
        historic_data = aquaspice_utils.query_historical_data_lastN(f'urn:ngsi-ld:AquaSpice:{entityType}:{short_id}', query_points)

        # Iterate over defined properties
        for variable in analysis["analyzedProperties"]:
            if variable != "location":
                entities_data["z_score_measurement_" + str(short_id)][variable] = {
                    "data": pd.DataFrame(columns=["date", str(variable)]),
                    "mean": None,
                    "std": None,
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
                    aux_func.logMessage("--> Creating dataframes based on historical data (Z-score).")

                    entities_data["z_score_measurement_" + str(short_id)][variable]["data"]["date"] = pd.to_datetime(
                        historic_data["index"])
                    entities_data["z_score_measurement_" + str(short_id)][variable]["data"][variable] = \
                    next(e for e in historic_data["attributes"] if e["attrName"] == variable)["values"]

                # Calculate mean and std metrics
                entities_data["z_score_measurement_" + str(short_id)][variable]["mean"] = np.mean(
                    entities_data["z_score_measurement_" + str(short_id)][variable]["data"][variable].values)
                entities_data["z_score_measurement_" + str(short_id)][variable]["std"] = np.std(
                    entities_data["z_score_measurement_" + str(short_id)][variable]["data"][variable].values)
                aux_func.logMessage(entities_data["z_score_measurement_" + str(short_id)][variable]["data"].head())

            aux_func.logMessage(f"---> Finished creating z-score variables for urn = {station_id}")

            # Free memory
            del historic_data

    return entities_data, anomaly_status
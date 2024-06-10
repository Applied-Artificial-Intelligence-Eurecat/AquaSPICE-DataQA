################################################################################### Imports
from datetime import datetime
import flask
import pandas as pd
import warnings
import numpy as np
from flask_apscheduler import APScheduler

import context_broker_client_utils as aquaspice_utils
import hampel_functions as hampel_func
import z_score_functions as zscore_func
import watercps_functions as wcps_func
import auxiliar_functions as aux_func

###################################################################################

warnings.filterwarnings(action = "ignore")

app = flask.Flask(__name__)

# Dict to hold metrics in memory (for Z-score)
entities_data = {}

# Dict to hold N-data in memory to calculate hampel filter
hampel_filter_measurements = {}

# Dict to hold in memory data of the watercps thresholds
watercps_measurements = {}

# Variable to hold and control the last date of received data.
last_date_received = {}
last_observedAt_received = {key: {} for key in ["urn:ngsi-ld:Subscription:hampel_anomaly_detection_1", "urn:ngsi-ld:Subscription:z_score_detection_1","urn:ngsi-ld:Subscription:watercps_detection_1"]}

# Variable to hold anomaly status
anomaly_status = {}

# Indicates wheter or not the dataframes on memory should be printed (for debug)
print_debug = False
produce_corrected_reading_debug = True

############################### Scheduler #########################################
# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

# Initialize scheduler
scheduler = APScheduler()
scheduler.start()
###################################################################################

@app.route("/", methods=["POST"])
def callback():
    '''
    Callback function
    '''
    aux_func.logMessage("--> Callback function called.")
    # For more info, see:
    # https://github.com/FIWARE/tutorials.LD-Subscriptions-Registrations

    # print the incoming json
    # print(json.dumps(flask.request.json, indent=4), flush=True)

    for reading in flask.request.json["data"]:
        process_reading(reading, flask.request.json["subscriptionId"], aux_func._produce_anomaly, _produce_corrected_reading)

    return flask.jsonify(isError=False, message="Success", statusCode=200), 200

################################################ Main function

def process_reading(reading, subscriptionId, produce_anomaly, produce_corrected_reading):
    """
    Main function to analyze incoming samples
    """
    global entities_data, hampel_filter_measurements, watercps_measurements, last_date_received, last_observedAt_received, anomaly_status

    analysis_list = aquaspice_utils.config["analysis"]

    # Check if the subscription is valid
    if subscriptionId in [x["subscription_id"] for x in analysis_list]:
        # Only calls after the first reading
        need_reset_dicts = False

        if reading["id"] in last_observedAt_received[subscriptionId]:
            need_reset_dicts = aux_func.calculate_date_distance(last_observedAt_received,
                                                                last_date_received,
                                                                subscriptionId,
                                                                reading["id"],
                                                                aux_func.return_observedAt(reading))

        # Updates the last time data was received (for each entityType)
        last_date_received[reading["id"]] = aux_func.get_datetime_now()
        last_observedAt_received[subscriptionId][reading["id"]] = aux_func.return_observedAt(reading)

        # Get short id
        short_id = reading["id"].split(":")[4]

        aux_func.logMessage(f"\n ################ New reading received ################ subscription_id = {subscriptionId}")
        aux_func.logMessage(f"observedAt: {last_observedAt_received[subscriptionId][reading['id']]}\n")

        ################################################ Analysis block

        # Get the analysis index (Identify analysis based on the subscription id)
        analysis_index = [x["subscription_id"] == subscriptionId for x in analysis_list].index(True)
        
        # Trigger create history, but querying historic data instead of starting from 0
        create_history(reading["id"], analysis_list[analysis_index], start_from_0 = False)

        property_correction = {}
        is_outlier = {}
        property_error_reason = {}
        
        # Cycle through the defined properties
        for property_name in analysis_list[analysis_index]["analyzedProperties"]:
            aux_func.logMessage(f"----> Initiated analysis for entity: {reading['id']}")
            aux_func.logMessage(f"----> property_name: {property_name} ({analysis_list[analysis_index]['algorithm']}), value: {reading[property_name]['value']}")
            
            property_correction[property_name] = {}
            is_outlier[property_name] = "No"
            property_error_reason[property_name] = "None"
            
            # To treat the first execution (when there is no data in memory in case of starting from 0)
            if analysis_list[analysis_index]["algorithm"] == "z_score":
                if entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"] is None:
                    manage_sliding_window_dataframe(station_id = reading["id"],
                                                    property_name = property_name,
                                                    algorithm = analysis_list[analysis_index]['algorithm'],
                                                    value = reading[property_name]["value"],
                                                    date = reading[property_name]["observedAt"],
                                                    dict_reset = True)

            # To treat the first execution (when there is no data in memory in case of starting from 0)
            if analysis_list[analysis_index]["algorithm"] == "watercps_threshold":
                if watercps_measurements["watercps_" + str(short_id)][property_name]["num_data"] is None:
                    manage_sliding_window_dataframe(station_id = reading["id"],
                                                    property_name = property_name,
                                                    algorithm = analysis_list[analysis_index]['algorithm'],
                                                    value = reading[property_name]["value"],
                                                    date = reading[property_name]["observedAt"],
                                                    dict_reset = True)

            # To treat the first execution (when there is no data in memory in case of starting from 0)
            if analysis_list[analysis_index]["algorithm"] == "hampel_filter":
                if hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["num_data"] is None:
                    manage_sliding_window_dataframe(station_id = reading["id"],
                                                    property_name = property_name,
                                                    algorithm = analysis_list[analysis_index]['algorithm'],
                                                    value = reading[property_name]["value"],
                                                    date = reading[property_name]["observedAt"],
                                                    dict_reset = True)
            
            # Reset dicts if needed
            if need_reset_dicts == True:
                manage_sliding_window_dataframe(reading["id"], property_name, analysis_list[analysis_index]['algorithm'], reading[property_name]["value"], reading[property_name]["observedAt"], True)
            
            # Identify algorithms
            if analysis_list[analysis_index]["algorithm"] == "z_score":
                is_outlier[property_name] = zscore_func.outlier_function_z_score(config = aquaspice_utils.config,
                                                                  station_id = reading["id"],
                                                                  variable = property_name,
                                                                  threshold = aquaspice_utils.config["z_score_threshold"],
                                                                  data = reading[property_name]["value"],
                                                                  entities_data = entities_data)

            elif analysis_list[analysis_index]["algorithm"] == "hampel_filter":
                is_outlier[property_name] = hampel_func.outlier_function_hampel_filter(config = aquaspice_utils.config,
                                                                        station_id = reading["id"],
                                                                        variable = property_name,
                                                                        data = reading[property_name]["value"],
                                                                        hampel_filter_measurements = hampel_filter_measurements)

            elif analysis_list[analysis_index]["algorithm"] == "watercps_threshold":
                is_outlier[property_name], property_error_reason[property_name] = wcps_func.outlier_function_watercps(config = aquaspice_utils.config["watercps_error_flagging"],
                                                                         station_id = reading["id"],
                                                                         variable = property_name,
                                                                         data = reading[property_name]["value"],
                                                                         watercps_measurements = watercps_measurements,
                                                                         debug = False)

            # If outlier    
            if (is_outlier[property_name] == True) or (is_outlier[property_name] == "Yes"):
                anomaly_start_date = None
                current_reading_date = reading[property_name]["observedAt"]
                
                if anomaly_status[reading["id"]][property_name]["startDateOfOngoingAnomaly"]:
                    anomaly_start_date = anomaly_status[reading["id"]][property_name]["startDateOfOngoingAnomaly"]
                else:
                    anomaly_start_date = current_reading_date
                    anomaly_status[reading["id"]][property_name]["startDateOfOngoingAnomaly"] = anomaly_start_date

                produce_anomaly(
                    reading["id"].split(":")[4]
                    + "_"
                    + property_name,
                    analysis_list[analysis_index]["entityType"],
                    analysis_list[analysis_index]["anomalyTypeId"],
                    anomaly_start_date,
                    current_reading_date,
                    "abnormal value in sensor: " + str(property_name),
                )
                
                if analysis_list[analysis_index]["algorithm"] == "z_score":
                    property_correction[property_name]["value"] = round(entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"], 2)
                    
                elif analysis_list[analysis_index]["algorithm"] == "hampel_filter":
                    property_correction[property_name]["value"] = round(np.mean(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"][property_name].values[-aquaspice_utils.config["property_sliding_window"][property_name]:]), 2)

                elif analysis_list[analysis_index]["algorithm"] == "watercps_threshold":
                    property_correction[property_name]["value"] = round(np.mean(watercps_measurements["watercps_" + str(short_id)][property_name]["data"][property_name].values[-4:]))

                aux_func.logMessage(f"--> {property_name} corrected value from {reading[property_name]['value']} to {property_correction[property_name]['value']}")
            
            else:
                # Clear the ongoing anomaly
                anomaly_status[reading["id"]][property_name]["startDateOfOngoingAnomaly"] = None
                
                # Use the original value (does not correct)
                property_correction[property_name]["value"] = reading[property_name]["value"]

            # Trigger update of in-memory data
            manage_sliding_window_dataframe(station_id=reading["id"],
                                            property_name=property_name,
                                            algorithm=analysis_list[analysis_index]['algorithm'],
                                            value=reading[property_name]["value"],
                                            date=reading[property_name]["observedAt"],
                                            dict_reset=False)

        # Answer back (with corrected values)
        produce_corrected_reading(id=reading["id"].split(":")[4],
                                  entityType=reading["type"],
                                  reading=reading,
                                  corrected_variables=property_correction,
                                  analysis=analysis_list[analysis_index],
                                  is_outlier=is_outlier,
                                  reason_watercps = property_error_reason)
    else:
        aux_func.logMessage(f"---X Unknown subscription, the incoming package is ignored: {subscriptionId}", "error")
        pass

def manage_sliding_window_dataframe(station_id, property_name, algorithm, value, date, dict_reset = False):
    '''
    Manage the in-memory dataframes.
    Decides when to reset the in-memory dataframes, and updates its values/index each time a sample is received
    '''
    global entities_data, hampel_filter_measurements, watercps_measurements, print_debug

    short_id = station_id.split(":")[4]
    new_row = {"date" : pd.to_datetime(date), str(property_name) : value}
    
    if dict_reset == False:
        if algorithm == "z_score":
            # Reset index
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].reset_index(inplace = True)
            
            try:
                entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except: pass
            
            # Append new row
            # New append format (Pandas 2.1.2): new_df = pd.concat([new_df, pd.Series({"Value" : 1, "Col1" : "Val1", "Col2" : "Val2"}).to_frame().T], ignore_index = True)
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = pd.concat([entities_data['z_score_measurement_' + str(short_id)][property_name]["data"], pd.Series(new_row).to_frame().T], ignore_index = True)

            # Set index again
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].set_index("date", inplace = True)

            # Drop index duplicates and sort index
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"][~entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].index.duplicated(keep='first')]
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].sort_index()
            
            # Trim dataset according to config
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"][-aquaspice_utils.config["query_points"]:]
            
            # Re-calculate metrics
            entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"] = np.mean(entities_data["z_score_measurement_" + str(short_id)][property_name]["data"][property_name][-((aquaspice_utils.config["property_sliding_window"][property_name] * 2) + 2):].values)
            entities_data["z_score_measurement_" + str(short_id)][property_name]["std"] = np.std(entities_data["z_score_measurement_" + str(short_id)][property_name]["data"][property_name][-((aquaspice_utils.config["property_sliding_window"][property_name] * 2) + 2):].values)
            
            if print_debug is True:
                aux_func.logMessage(entities_data['z_score_measurement_' + str(short_id)][property_name]["data"], kind = "debug")
            
            try:
                entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except:
                pass
            
        elif algorithm == "hampel_filter":
            
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].reset_index(inplace = True)
            
            try:
                hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except: pass

            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = pd.concat([hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"], pd.Series(new_row).to_frame().T], ignore_index = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"][~hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].index.duplicated(keep='first')]
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].sort_index()
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"][-aquaspice_utils.config["query_points"]:]
            
            if print_debug is True:
                aux_func.logMessage(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"])
            
            try:
                hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except:
                pass
            
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["num_data"] = len(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"])

        elif algorithm == "watercps_threshold":
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"].reset_index(inplace=True)

            try:
                watercps_measurements["watercps_" + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except: pass

            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = pd.concat(
                [watercps_measurements["watercps_" + str(short_id)][property_name]["data"],
                 pd.Series(new_row).to_frame().T], ignore_index=True)

            watercps_measurements["watercps_" + str(short_id)][property_name]["data"].set_index("date", inplace=True)
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = watercps_measurements["watercps_" + str(short_id)][property_name]["data"][~watercps_measurements["watercps_" + str(short_id)][property_name]["data"].index.duplicated(keep='first')]
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = watercps_measurements["watercps_" + str(short_id)][property_name]["data"].sort_index()
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = watercps_measurements["watercps_" + str(short_id)][property_name]["data"][-aquaspice_utils.config["query_points"]:]

            if print_debug is True:
                aux_func.logMessage(watercps_measurements["watercps_" + str(short_id)][property_name]["data"], kind = "debug")

            try:
                watercps_measurements["watercps_" + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace=True)
            except:
                pass

            watercps_measurements["watercps_" + str(short_id)][property_name]["num_data"] = len(watercps_measurements["watercps_" + str(short_id)][property_name]["data"])

    # Reset on-memory dicts
    elif dict_reset == True:
        aux_func.logMessage("--> Triggered on-memory reset dicts")
        if algorithm == "z_score":
            entities_data["z_score_measurement_" + str(short_id)][property_name]["data"] = pd.DataFrame(columns = ["date", str(property_name)])
            entities_data["z_score_measurement_" + str(short_id)][property_name]["data"] = pd.concat([entities_data["z_score_measurement_" + str(short_id)][property_name]["data"], pd.Series(new_row).to_frame().T], ignore_index = True)
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"] = value
            entities_data["z_score_measurement_" + str(short_id)][property_name]["std"] = value
            
        elif algorithm == "hampel_filter":
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = pd.DataFrame(columns = ["date", str(property_name)])
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = pd.concat([hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"], pd.Series(new_row).to_frame().T], ignore_index = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["num_data"] = len(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"])

        elif algorithm == "watercps_threshold":
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = pd.DataFrame(columns = ["date", str(property_name)])
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"] = pd.concat([watercps_measurements["watercps_" + str(short_id)][property_name]["data"], pd.Series(new_row).to_frame().T], ignore_index = True)
            watercps_measurements["watercps_" + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            watercps_measurements["watercps_" + str(short_id)][property_name]["num_data"] = len(watercps_measurements["watercps_" + str(short_id)][property_name]["data"])

def create_history(station_id, analysis, start_from_0 = False):
    '''
    Function which decides if the entity Id is on the in-memory dicts
    If the entity is already on memory, it passes, if not, triggers the respective module for each kind of analysis
    '''
    global entities_data, hampel_filter_measurements, watercps_measurements, last_observedAt_received, anomaly_status
    
    short_id = station_id.split(":")[4]
    
    if analysis["algorithm"] == "z_score":
        if "z_score_measurement_" + str(short_id) in entities_data:
            pass
        else:
            aux_func.logMessage(f"--> (create_history): Id {short_id} triggered z_score module.")
            entities_data, anomaly_status = zscore_func.z_score_module(aquaspice_utils.config,
                                                                       station_id,
                                                                       analysis,
                                                                       entities_data,
                                                                       anomaly_status,
                                                                       start_from_0)
            
    elif analysis["algorithm"] == "hampel_filter":
        if "hampel_filter_" + str(short_id) in hampel_filter_measurements:
            pass
        else:
            aux_func.logMessage(f"--> (create_history): Id {short_id} triggered hampel filter module.")
            hampel_filter_measurements, anomaly_status = hampel_func.hampel_filter_module(aquaspice_utils.config,
                                                                                          station_id,
                                                                                          analysis,
                                                                                          hampel_filter_measurements,
                                                                                          anomaly_status,
                                                                                          start_from_0)

    elif analysis["algorithm"] == "watercps_threshold":
        if "watercps_" + str(short_id) in watercps_measurements:
            pass
        else:
            aux_func.logMessage(f"--> (create_history): Id {short_id} triggered watercps module.")
            watercps_measurements, anomaly_status = wcps_func.watercps_module(aquaspice_utils.config,
                                                                              station_id,
                                                                              analysis,
                                                                              watercps_measurements,
                                                                              anomaly_status,
                                                                              start_from_0)

############### Support functions ###############

def _produce_corrected_reading(id, entityType, reading, corrected_variables, analysis, is_outlier, reason_watercps):
    """
    Sends corrected_data to the context broker
    """
    global produce_corrected_reading_debug
    
    if (str(entityType) == "measurementStation") or (str(entityType) == "measurementstation"):
        entityType = "MeasurementStation"

    # Get correct flag
    flag = "Flagged" if analysis["algorithm"] == "watercps_threshold" else "Corrected"

    aux_func.logMessage("--> Started produce_corrected_reading")
    body = [
        {
            "id": "urn:ngsi-ld:AquaSpice:" + str(entityType) + str(flag) + ":" + id,
            "type":  str(entityType) + str(flag)
        }
    ]

    for property_name in analysis["notCorrectedProperties"]:
        if property_name in reading:
            body[0][property_name] = reading[property_name]
        
    # Exclude properties that are not relevant    
    properties_to_iterate = [str(x) for x in reading.keys() if not x in analysis["notCorrectedProperties"]]
    
    if produce_corrected_reading_debug == True:
        aux_func.logMessage(f"Analysis: {analysis['algorithm']}", kind = "debug")
        aux_func.logMessage(f"Properties to iterate: {properties_to_iterate}", kind = "debug")
        aux_func.logMessage(f"analyzedProperties: {analysis['analyzedProperties']}", kind = "debug")

    # Add corresponding properties to the corrected reading, propertyRaw and propertyCorrected
    if analysis["algorithm"] == "watercps_threshold":
        for property_name in properties_to_iterate:
            body[0][str(property_name)] = {
                "type": "Property",
                "value": reading[property_name]["value"],
                "observedAt": reading[property_name]["observedAt"]
            }

            body[0][str(property_name) + "_error"] = {
                "type": "Property",
                "value": str(is_outlier[property_name]),  # Yes or No
                "observedAt": reading[property_name]["observedAt"]
            }

            body[0][str(property_name) + "_error_reason"] = {
                "type": "Property",
                "value": str(reason_watercps[property_name]),  # Can be reason_max, min, and delta.
                "observedAt": reading[property_name]["observedAt"]
            }
    else:
        for property_name in properties_to_iterate:
            body[0][str(property_name) + "Raw"] = {
                "type" : "Property",
                "value" : reading[property_name]["value"],
                "observedAt" : reading[property_name]["observedAt"]
            }

            body[0][str(property_name) + "Corrected"] = {
                "type" : "Property",
                "value" : corrected_variables[property_name]["value"],
                "observedAt" : reading[property_name]["observedAt"]
            }

    body[0]["@context"] =  [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ]

    # Print body
    aux_func.logMessage(f"--> Upsert body debug: {body}")

    aquaspice_utils.upsert_context_broker(body)

@scheduler.task("interval", id = "cadency_check", seconds = 3000, misfire_grace_time = 300)
def cadency_monitoring():
    """
    Function to monitor if data has been received the last x minutes (configured in data_qa_params.json)
    The function is executed every 45min.
    misfire_grace_time = (None means “allow the job to run no matter how late it is”)
    """
    global last_date_received

    aux_func.logMessage(f"---- Cadency time anomaly module triggered ---- (current threshold) : {aquaspice_utils.config['data_cadency_anomaly_threshold']} (minutes)")

    # Check all the current measured stations
    for key in last_date_received:
        difference = datetime.strptime(
            aux_func.get_datetime_now(), "%Y-%m-%dT%H:%M:%SZ"
        ) - datetime.strptime(last_date_received[key], "%Y-%m-%dT%H:%M:%SZ")

        # Calculate difference in minutes
        difference = difference.total_seconds() / 60

        if difference > aquaspice_utils.config["data_cadency_anomaly_threshold"]:
            # Trigger anomaly alert
            produce_anomaly(str(key), get_datetime_now(), "cadency of data (>20min)")
            aux_func.logMessage(f"Anomaly: data of {str(key).split(':')[4]}: hasn't been received for: {str(difference)} minutes.", kind = "warning")
        else:
            pass

if __name__ == "__main__":
    '''
    Main function, initiate process.
    '''

    aquaspice_utils.init()

    # Create subscriptions
    aux_func.logMessage("--> Subscribing to defined topics (json file)...")
    
    # Cycle through defined analysis in json file
    # Create subscription at given ID (get from config file) for each analysis
    for analysis in aquaspice_utils.config["analysis"]:
        aquaspice_utils.create_subscription(analysis)
                
    # Run application
    aux_func.logMessage("--> streaming_analysis started")
    app.run(host="0.0.0.0")
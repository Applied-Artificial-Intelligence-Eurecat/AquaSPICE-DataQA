import datetime, json, flask, math
import context_broker_client_utils as context_broker_utils
import numpy as np
import pandas as pd
from hampel import hampel
from flask_apscheduler import APScheduler

import warnings
warnings.filterwarnings(action = "ignore")

###################################################################################

app = flask.Flask(__name__)

# Dict to hold metrics in memory (for Z-score)
entities_data = {}

# Dict to hold N-data in memory to calculate hampel filter
hampel_filter_measurements = {}

# Variable to hold and control the last date of received data.
last_date_received = {}
last_observedAt_received = {key: {} for key in ["urn:ngsi-ld:Subscription:hampel_anomaly_detection_1", "urn:ngsi-ld:Subscription:z_score_detection_1"]}

# Variable to hold anomaly status
anomaly_status = {}

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
    print("--> Callback function called.")
    # For more info, see:
    # https://github.com/FIWARE/tutorials.LD-Subscriptions-Registrations

    # print the incoming json
    # print(json.dumps(flask.request.json, indent=4), flush=True)

    for reading in flask.request.json["data"]:
        process_reading(reading, flask.request.json["subscriptionId"], _produce_anomaly, _produce_corrected_reading)

    return flask.jsonify(isError=False, message="Success", statusCode=200), 200

def process_reading(reading, subscriptionId, produce_anomaly, produce_corrected_reading):
    """
    Main function that analyzes the incoming data and process them looking for outliers
    """
    global entities_data, hampel_filter_measurements, last_date_received, last_observedAt_received, anomaly_status
    
    # Only calls after the first reading
    need_reset_dicts = False
    
    if reading["id"] in last_observedAt_received[subscriptionId]:
        need_reset_dicts = calculate_date_distance(subscriptionId, reading["id"], return_observedAt(reading))

    # Updates the last time data was received (for each entityId)
    last_date_received[reading["id"]] = get_datetime_now()
    last_observedAt_received[subscriptionId][reading["id"]] = return_observedAt(reading)
    
    short_id = reading["id"].split(":")[4]
    
    property_correction = {}
    
    print(f"\n ######## New reading received ######## subscription_id = {subscriptionId}\n")
    
    analysis_list = context_broker_utils.config["analysis"]
    
    ################################################ New block to identify analysis
    
    # Check if the subscription is valid
    if subscriptionId in [x["subscription_id"] for x in analysis_list]:
        # Get the analysis index (Identify analysis based on the subscription id)
        analysis_index = [x["subscription_id"] == subscriptionId for x in analysis_list].index(True)
        
        # Trigger create history
        create_history(reading["id"], analysis_list[analysis_index], start_from_0 = True)
        
        # Cycle through the defined properties
        for property in analysis_list[analysis_index]["analyzedProperties"]:
            print(f"----> Initiated analysis for entity: {reading['id']}")
            print(f"----> Property: {property} ({analysis_list[analysis_index]['algorithm']})")
            property_correction[property] = {}
            is_outlier = False
            
            # To treat the first execution (when there is no data in memory)
            if analysis_list[analysis_index]["algorithm"] == "z_score":
                if entities_data["z_score_measurement_" + str(short_id)][property]["mean"] == None:
                    manage_sliding_window_dataframe(reading["id"], property, analysis_list[analysis_index]['algorithm'], reading[property]["value"], reading[property]["observedAt"], True)

            if analysis_list[analysis_index]["algorithm"] == "hampel_filter": 
                if hampel_filter_measurements["hampel_filter_" + str(short_id)][property]["num_data"] == None:
                    manage_sliding_window_dataframe(reading["id"], property, analysis_list[analysis_index]['algorithm'], reading[property]["value"], reading[property]["observedAt"], True)
            
            # Reset dicts
            if need_reset_dicts == True:
                manage_sliding_window_dataframe(reading["id"], property, analysis_list[analysis_index]['algorithm'], reading[property]["value"], reading[property]["observedAt"], True)
            
            # Identify algorithms
            if analysis_list[analysis_index]["algorithm"] == "z_score":
                is_outlier = outlier_function_z_score(reading["id"],
                                        property,
                                        context_broker_utils.config["z_score_threshold"],
                                        reading[property]["value"])
                
            elif analysis_list[analysis_index]["algorithm"] == "hampel_filter":
                is_outlier = outlier_function_hampel_filter(reading["id"], 
                                                            property, 
                                                            reading[property]["value"])
                        
            # If outlier    
            if is_outlier == True:
                anomaly_start_date = None
                current_reading_date = reading[property]["observedAt"]
                
                if anomaly_status[reading["id"]][property]["startDateOfOngoingAnomaly"]:
                    anomaly_start_date = anomaly_status[reading["id"]][property]["startDateOfOngoingAnomaly"]
                else:
                    anomaly_start_date = current_reading_date
                    anomaly_status[reading["id"]][property]["startDateOfOngoingAnomaly"] = anomaly_start_date
                
                # Trigger function to produce anomaly
                produce_anomaly(
                    reading["id"].split(":")[4]
                    + "_"
                    + property,
                    analysis_list[analysis_index]["entityType"],
                    analysis_list[analysis_index]["anomalyTypeId"],
                    anomaly_start_date,
                    current_reading_date,
                    "abnormal value in sensor: " + str(property),
                )
                
                if analysis_list[analysis_index]["algorithm"] == "z_score":
                    property_correction[property]["value"] = entities_data["z_score_measurement_" + str(short_id)][property]["mean"]
                    
                elif analysis_list[analysis_index]["algorithm"] == "hampel_filter":
                    property_correction[property]["value"] = np.mean(hampel_filter_measurements["hampel_filter_" + str(short_id)][property]["data"][property].values[-context_broker_utils.config["property_sliding_window"][property]:])
                
                print(f"--> {property} corrected value from {reading[property]['value']} to {property_correction[property]['value']}")
            
            else:
                # Clear the ongoing anomaly
                anomaly_status[reading["id"]][property]["startDateOfOngoingAnomaly"] = None
                
                # Use the original value (does not correct)
                property_correction[property]["value"] = reading[property]["value"]
                
            # Trigger update in in-memory data
            manage_sliding_window_dataframe(reading["id"], property, analysis_list[analysis_index]['algorithm'], reading[property]["value"], reading[property]["observedAt"])
            
        # Answer back (with corrected values)
        produce_corrected_reading(
            reading["id"].split(":")[4],
            reading["type"],
            reading,
            property_correction,
            analysis_list[analysis_index])
    else:
        print("---X Unknown subscription, the incoming package is ignored")
        pass

# todo
def manage_sliding_window_dataframe(station_id, property_name, algorithm, value, date, dict_reset = False):
    '''
    '''
    global entities_data, hampel_filter_measurements
    print("Entrou manage sliding window dataframe.")
    
    short_id = station_id.split(":")[4]
    new_row = {"date" : pd.to_datetime(date), str(property_name) : value}
    
    if dict_reset == False:
        if algorithm == "z_score":
            # Reset index
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].reset_index(inplace = True)
            
            # Append new row
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].append(new_row, ignore_index = True)
            # Set index again
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            # Resample data 
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].resample("15Min").mean().ffill()
            # Trim dataset according to config
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"] = entities_data['z_score_measurement_' + str(short_id)][property_name]["data"][-context_broker_utils.config["query_points"]:]
        
            # Re-calculate metrics
            entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"] = np.mean(entities_data["z_score_measurement_" + str(short_id)][property_name]["data"][property_name][-((context_broker_utils.config["property_sliding_window"][property_name] * 2) + 2):].values)
            entities_data["z_score_measurement_" + str(short_id)][property_name]["std"] = np.std(entities_data["z_score_measurement_" + str(short_id)][property_name]["data"][property_name][-((context_broker_utils.config["property_sliding_window"][property_name] * 2) + 2):].values)
            
            print(entities_data['z_score_measurement_' + str(short_id)][property_name]["data"])
            
            try:
                entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except:
                pass
        elif algorithm == "hampel_filter":
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].reset_index(inplace = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].append(new_row, ignore_index = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].resample("15Min").mean().ffill()
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"][-context_broker_utils.config["query_points"]:]
            
            print(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"])
            
            try:
                hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].drop("index", axis = 1, inplace = True)
            except:
                pass
            
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["num_data"] = len(hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"])
    
    # Reset on-memory dicts
    elif dict_reset == True:
        print("--> Triggered on-memory reset dicts")
        if algorithm == "z_score":
            entities_data["z_score_measurement_" + str(short_id)][property_name]["data"] = pd.DataFrame(columns = ["date", str(property_name)])
            entities_data["z_score_measurement_" + str(short_id)][property_name]["data"] = entities_data["z_score_measurement_" + str(short_id)][property_name]["data"].append(new_row, ignore_index = True)
            entities_data['z_score_measurement_' + str(short_id)][property_name]["data"].set_index("date", inplace = True)
            entities_data["z_score_measurement_" + str(short_id)][property_name]["mean"] = value
            entities_data["z_score_measurement_" + str(short_id)][property_name]["std"] = value
            
        elif algorithm == "hampel_filter":
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = pd.DataFrame(columns = ["date", str(property_name)])
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"] = hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].append(new_row, ignore_index = True)
            hampel_filter_measurements["hampel_filter_" + str(short_id)][property_name]["data"].set_index("date", inplace = True)
    
def create_history(station_id, analysis, start_from_0 = False):
    '''
    Function which decide if the entity Id is on the in-memory dicts
    '''
    global entities_data, hampel_filter_measurements, last_observedAt_received
    
    short_id = station_id.split(":")[4]
    
    if analysis["algorithm"] == "z_score":
        if "z_score_measurement_" + str(short_id) in entities_data:
            # do nothing
            #print(f"--> (create_history): Id {short_id} ignored because its historic data is in memory already.")
            pass
        else:
            print(f"--> (create_history): Id {short_id} triggered z_score module.")
            
            if start_from_0 == True:
                z_score_module(station_id, analysis, True)
            elif start_from_0 == False:
                z_score_module(station_id, analysis)
            
    elif analysis["algorithm"] == "hampel_filter":
        if "hampel_filter_" + str(short_id) in hampel_filter_measurements:
            # do nothing
            #print(f"--> (create_history): Id {short_id} ignored because its historic data is in memory already.")
            pass
        else:
            print(f"--> (create_history): Id {short_id} triggered hampel filter module.")
            
            if start_from_0 == True:
                hampel_filter_module(station_id, analysis, True)
            elif start_from_0 == False:
                hampel_filter_module(station_id, analysis)

def z_score_module(station_id, analysis, start_from_0 = False):
    '''
    Process responsible for doing the initial population of in-memory dicts.
    '''
    global entities_data, anomaly_status
    print('--> Started z-score module.')
    
    query_points = context_broker_utils.config["query_points"]
            
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
        for property in analysis["analyzedProperties"]:
            if property != "location":
                entities_data["z_score_measurement_" + str(short_id)][property] = {
                    "data": pd.DataFrame(columns = ["date", str(property)]),
                    "mean": None,
                    "std": None,
                    "startDateOfOngoingAnomaly": None,
                }
                
                # Anomaly control
                if property in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][property] = {}
                    anomaly_status[station_id][property]["startDateOfOngoingAnomaly"] = None
                    
    elif start_from_0 == False:
        # Query last-N data points
        # uncomment for production
        #historic_data = query_historical_data(station_id, query_points)
        historic_data = query_historical_all_data(f'urn:ngsi-ld:Project:{entityType}:{short_id}', offset=0, limit=query_points)
    
        # Iterate over defined properties
        for property in analysis["analyzedProperties"]:
            if property != "location":
                entities_data["z_score_measurement_" + str(short_id)][property] = {
                    "data": pd.DataFrame(columns = ["date", str(property)]),
                    "mean": None,
                    "std": None,
                    "startDateOfOngoingAnomaly": None,
                }
                
                # Anomaly control
                if property in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][property] = {}
                    anomaly_status[station_id][property]["startDateOfOngoingAnomaly"] = None
                
                # Populate dict with in-memory data
                # If pandas sld windows is active, need to revise that.
                if historic_data != None:
                    entities_data["z_score_measurement_" + str(short_id)][property]["data"] = next(e for e in historic_data["attributes"] if e["attrName"] == property)["values"]

                # Calculate mean and std metrics
                entities_data["z_score_measurement_" + str(short_id)][property]["mean"] = np.mean(entities_data["z_score_measurement_" + str(short_id)][property]["data"])
                entities_data["z_score_measurement_" + str(short_id)][property]["std"] = np.std(entities_data["z_score_measurement_" + str(short_id)][property]["data"])
                    
            
            print(f"---> Finished creating z-score propertys for urn = {station_id}")
        
            # Free memory
            del historic_data
        
def hampel_filter_module(station_id, analysis, start_from_0 = False):
    '''
    Module responsible for executing the initial in-memory population of data for the hampel filter algorithm to work
    '''
    print('--> Started hampel_filter module.')
    
    global hampel_filter_measurements, anomaly_status
    
    query_points = (context_broker_utils.config["hampel_sliding_window"][3] * 2) + 2
            
    # Get shorter measurement station id (for dictionary naming)
    short_id = station_id.split(":")[4]
    
    hampel_filter_measurements["hampel_filter_" + str(short_id)] = {}
    
    if station_id in anomaly_status:
        pass
    else:
        anomaly_status[station_id] = {}
    
    if start_from_0 == True:
        for property in analysis["analyzedProperties"]:
            if property != "location":
                hampel_filter_measurements["hampel_filter_" + str(short_id)][property] = {
                    "data": [],
                    "num_data": None,
                    "date_updated" : get_datetime_now()
                }
                
                # Anomaly control
                if property in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][property] = {}
                    anomaly_status[station_id][property]["startDateOfOngoingAnomaly"] = None
                    
    elif start_from_0 == False:
        # Get historic data
        # uncomment for production
        #historic_data = query_historical_data(station_id, query_points)
        historic_data = query_historical_all_data(f'urn:ngsi-ld:Project:{entityType}:{short_id}', offset=0, limit=query_points)
        
        # Iterate over properties defined on config file
        for property in analysis["analyzedProperties"]:
            if property != "location":
                hampel_filter_measurements["hampel_filter_" + str(short_id)][property] = {
                    "data": pd.DataFrame(columns = ["date", str(property)]),
                    "num_data": None,
                    "date_updated" : get_datetime_now()
                }
                
                # Anomaly control
                if property in anomaly_status[station_id]:
                    pass
                else:
                    anomaly_status[station_id][property] = {}
                    anomaly_status[station_id][property]["startDateOfOngoingAnomaly"] = None
                
                # Populate dicts
                if historic_data != None:
                    hampel_filter_measurements["hampel_filter_" + str(short_id)][property]["data"] = next(e for e in historic_data["attributes"] if e["attrName"] == property)["values"]
                    
                    # Get the number of available data samples
                    hampel_filter_measurements["hampel_filter_" + str(short_id)][property]["num_data"] = len(hampel_filter_measurements["hampel_filter_" + str(short_id)][property]["data"])        

        print(f"---> Finished creating propertys for urn = {station_id}")

        # Free memory
        del historic_data       
        
def outlier_function_hampel_filter(station_id, variable, data):
    """
    Function to implement Hampel Filter method
    """
    global hampel_filter_measurements
    
    short_id = station_id.split(":")[4]
    
    data_list = hampel_filter_measurements["hampel_filter_" + str(short_id)][variable]["data"][variable].values
    
    # Append data point and get its index
    data_list = list(data_list)[-(context_broker_utils.config["property_sliding_window"][variable]):]
    print(f"Len data list (start): {len(data_list)}")
    
    # Append sample of interest
    data_list.append(data)
    
    # Get index of the value
    index_data_point = len(data_list) - 1 if len(data_list) >= 2 else 0
    
    # Look at the lenght of available data, and adjust the sliding window size according to that
    hampel_window_size = math.floor((len(data_list) / 2))
    
    if hampel_window_size > context_broker_utils.config["property_sliding_window"][variable]:
        hampel_window_size = context_broker_utils.config["property_sliding_window"][variable]
        
    # Debug    
    print(f"Len data list (end): {len(data_list)}")
    if len(data_list) >= 2:
        print(f"--> Debug: min({np.min(data_list)}), max({np.max(data_list)}), mean({np.mean(data_list)}) current value: {data}, sliding_window: {hampel_window_size}")
    

    #if hampel_window_size >= context_broker_utils.config["property_sliding_window"][variable]:
    if hampel_window_size >= 100:
        # Run outlier detection, must convert to pd Series
        detected_outliers = hampel(pd.Series(data_list), window_size = hampel_window_size, n = 5, imputation = False)
        print(f"Total outliers in detected_outliers: {len(detected_outliers)}")

        del data_list
        
        # Debug
        # Check if the data point is an outlier or not
        if index_data_point in detected_outliers:
            print(f"--> Outlier ({data}) detected by hampel filter.====================================================")
            return True
        else:
            print(f"--> Hampel filter did not consider {data} an outlier.")
            return False
    else:
        return False
    
def outlier_function_z_score(station_id, variable, threshold, data):
    """
    Function to implement z-score method
    """
    global entities_data
    
    short_id = station_id.split(":")[4]
    #print(entities_data['z_score_measurement_' + str(short_id)][variable]['data'])
    print(f"Len data list: {len(entities_data['z_score_measurement_' + str(short_id)][variable]['data'])}")
    print(f"--> Debug: mean({entities_data['z_score_measurement_' + str(short_id)][variable]['mean']}) current value: {data}")
    
    if len(entities_data['z_score_measurement_' + str(short_id)][variable]['data']) >= 250:
        mean = entities_data["z_score_measurement_" + str(short_id)][variable]["mean"]
        std = entities_data["z_score_measurement_" + str(short_id)][variable]["std"]
        
        z = np.abs((data - mean) / std)

        # If it is an outlier return True, otherwise False
        outlier = True if z >= threshold else False
        
        # Debug
        if outlier == False:
            print(f"--> Z-Score did not consider {data} an outlier.")
        elif outlier == True:
            print(f"--> Z-score considered {data} an outlier.====================================================")
            
        return outlier
    else:
        return False
        
############### Support functions ###############

def get_datetime_now():
    """
    Return current date with correct format
    """
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def return_observedAt(reading):
    '''
    Func to return the first observedAt that is found
    # Use time_index if available?
    '''
    observedAt = None
    
    for property in ["conductivity", "temperature", "depth"]:
        try:
            observedAt = reading[property]["observedAt"]
        except:
            pass
        
    return str(observedAt).split(".")[0] + "Z"

def calculate_date_distance(subscriptionId, station_id, current_date):
    '''
    Calculate distance between dates, to trigger in-memory resets
    '''        
    global last_observedAt_received, last_date_received
    try:
        last_date = last_observedAt_received[subscriptionId][station_id]
        print(f"### Debug date: current_date: {current_date} last date received {last_date}")
        
        format = "%Y-%m-%dT%H:%M:%SZ"

        difference = datetime.datetime.strptime(current_date, format) - datetime.datetime.strptime(last_date, format)
        
        minutes = int(difference.total_seconds() / 60)
        
        print(f"The difference is: {minutes} minutes")
        
        # If it has a data gap greater than {x} minutes, order a reset in the in-memory dicts
        if minutes >= 180:
            # Do something, reset in memory dicts
            return True
        else:
            return False
        
    except Exception as e:
        print(f"Exception on calculating date distance: {e}")
        pass

def _produce_anomaly(id, entityType, anomalyTypeId, anomaly_start_date, last_anomaly_date, subject):
    """
    Insert a new anomaly sample in the context broker
    """
    
    print("--> Produce anomaly triggered")
    body = [
        {
            "id": "urn:ngsi-ld:Project:" + str(entityType) + "Corrected:" + str(anomalyTypeId) + ":" + id,
            "type": "Anomaly",
            "name": "value-anomaly",
            "description": "Something is wrong with: " + str(subject),
            "dateObserved": {
                "type": "Property",
                "value": {"@type": "DateTime", "@value": last_anomaly_date},
            },
            "validFrom": {
                "type": "Property",
                "value": {"@type": "DateTime", "@value": anomaly_start_date},
            },
            "validTo": {
                "type": "Property",
                "value": {"@type": "DateTime", "@value": last_anomaly_date},
            },
            "dateCreated": {
                "type": "Property",
                "value": {"type": "DateTime", "value": get_datetime_now()},
            },
            "dateIssued": {
                "type": "Property",
                "value": {"type": "DateTime", "value": get_datetime_now()},
            },
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"                
            ]
        }
    ]
    #print(body)
    context_broker_utils.upsert_context_broker(body)

def _produce_corrected_reading(
    id,
    entityType,
    reading,
    corrected_variables,
    analysis):
    """
    Sends corrected_data to the context broker
    """
     
    print("--> Started produce_corrected_reading")
    body = [
        {
            "id": "urn:ngsi-ld:Project:" + str(entityType) + "Corrected:" + id,
            "type":  str(entityType) + "Corrected"
        }
    ]
    
    # Podem tenir un parametre que anomenat {{notCorrectedProperties}} en la config de cada job que inclogui la llista de propietats 
    # que no s'han d'analitzar. Per cada una d'elles simplement copiarem la propietat del objecte original al objecte nou mantenint el nom.
    for property in analysis["notCorrectedProperties"]:
        if property in reading:
            body[0][property] = reading[property]
        
    # Exclude properties that are not relevant    
    properties_to_iterate = [str(x) for x in reading.keys() if not x in analysis["notCorrectedProperties"]]
    properties_to_iterate = [x for x in properties_to_iterate if not x in ["id", "type", "time_index", "location_centroid", "@context"]]
        
    # Add corresponding properties to the corrected reading, propertyRaw and propertyCorrected
    for property in properties_to_iterate:
        body[0][str(property) + "Raw"] = {
            "type" : "Property",
            "value" : reading[property]["value"],
            "observedAt" : reading[property]["observedAt"]
        }
        # If the property is in {{analyzedProperties}} 
        if property in analysis["analyzedProperties"]:
            body[0][str(property) + "Corrected"] = {
                "type" : "Property",
                "value" : corrected_variables[property]["value"],
                "observedAt" : reading[property]["observedAt"]
            }
        # If not, just use the original value
        else:
            body[0][str(property) + "Corrected"] = {
                "type" : "Property",
                "value" : reading[property]["value"],
                "observedAt" : reading[property]["observedAt"]
            }            
    
    # Add context
    # Correct one
    
    body[0]["@context"] =  [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"                
            ]
    context_broker_utils.upsert_context_broker(body)

def create_subscription(analysis):
    '''
    Function responsible of creating the subscription, based on the defined analysis (entitytype, ids, properties...)
    '''    
    # Create json list to specify which Ids/entities are part of this subscription
    entity_json_list = []
    
    if "entityIds" in analysis:
        for entity in analysis["entityIds"].split(";"):
            entity_json_list.append({"type" : analysis["entityType"], "id" : entity})
    else:
        entity_json_list.append({"type" : analysis["entityType"]})
        
    # Create new subscription
    body = {
        "id": analysis["subscription_id"],
        "description": "anomaly-detection",
        "type": "Subscription",
        "entities": entity_json_list,
        "notification": {
            "attributes": analysis["analyzedProperties"],
            "format": "normalized",
            "endpoint": {
                "uri": context_broker_utils.config["callback_url"],
                "accept": "application/json",
            },
        },
        # "throttling": 1,
        "@context": [
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
            {
                analysis["entityType"] : context_broker_utils.config[
                    "rtm_platform_public_url"
                ]
                + "/schemas/Project/" + analysis["entityType"] + "/schema.json"
            },
        ],
    }
    context_broker_utils.create_subscription(analysis["subscription_id"], body)
    
    del entity_json_list

    print("--> Subscription created")
    
def query_historical_all_data(urn, offset, limit):
    response = context_broker_utils.query_quantumleap(f'entities/{urn}?&type={URL}/schema.json&offset={offset}&limit={limit}') 
    
    if response.ok == False:
        print(f"---X Failed to get historical data for {urn}: {response.reason}")
        return None
    else:
        print(f"---> Query historical data at {urn} successful ({limit} samples retrieved).")
        return response.json()

@scheduler.task("interval", id = "cadency_check", seconds = 1500, misfire_grace_time = 300)
def cadency_monitoring():
    """
    Function to monitor if data has been received the last x minutes
    The function is executed every 30min
    misfire_grace_time = (None means “allow the job to run no matter how late it is”)
    """
    global last_date_received

    print(f"---- Cadency time anomaly module triggered ---- (current threshold) : {context_broker_utils.config['data_cadency_anomaly_threshold']} (minutes)")

    # Check all the current measured stations
    for key in last_date_received:
        difference = datetime.datetime.strptime(
            get_datetime_now(), "%Y-%m-%dT%H:%M:%SZ"
        ) - datetime.datetime.strptime(last_date_received[key], "%Y-%m-%dT%H:%M:%SZ")

        # Calculate difference in minutes
        difference = difference.total_seconds() / 60

        if difference > context_broker_utils.config["data_cadency_anomaly_threshold"]:
            # Trigger anomaly alert
            #produce_anomaly(str(key), get_datetime_now(), "cadency of data (>20min)")
            print(f"-X Anomaly of received time DETECTED on {str(key)}: Difference of last received data: {str(difference)}")
        else:
            #print(f"-> Anomaly of received time NOT detected on {str(key)} : Difference of last received data: {str(difference)}")
            pass

if __name__ == "__main__":
    context_broker_utils.init()
    
    # Create subscriptions
    print("--> Subscribe to defined topics (json file")
    
    # Cycle through defined analysis in json file
    # Create subscription at given ID (get from config file) for each analysis
    for analysis in context_broker_utils.config["analysis"]:
        create_subscription(analysis)
                
    # Run application
    print("--> Start")
    app.run(host="0.0.0.0")
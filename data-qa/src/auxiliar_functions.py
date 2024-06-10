################################################################################### Imports
import numpy as np
from datetime import datetime, timezone
import os

import context_broker_client_utils as aquaspice_utils

working_dir = os.path.dirname(os.path.realpath(__file__))

def iqr_method(observations, data_sample, iqr_threshold = 2):
    '''
    Implements IQR Method
    Default threshold = 1.5
    '''
    q1 = np.percentile(observations, 25) # Q1
    q3 = np.percentile(observations, 75) # Q3
    iqr = q3 - q1 
    
    # Observations > Q3 + 1.5 * IQR or Q1 - 1.5 * IQR
    if (data_sample > (q3 + (iqr_threshold * iqr))) or (data_sample < (q1 - (iqr_threshold * iqr))):
        logMessage("---> IQR decided True")
        return True
    else:
        logMessage("---> IQR decided False")
        return False

def return_observedAt(reading, properties):
    '''
    Func to return the first observedAt that is found
    # Use time_index if available?
    '''
    observedAt = None
    
    for property in properties:
        try:
            observedAt = reading[property]["observedAt"]
        except:
            pass
        
    return str(observedAt).split(".")[0] + "Z"


def _produce_anomaly(id, entityType, anomalyTypeId, anomaly_start_date, last_anomaly_date, subject):
    """
    Insert a new anomaly sample in the context broker
    """

    print("--> Produce anomaly triggered", flush = True)
    body = [
        {
            "id": "urn:ngsi-ld:AquaSpice:" + str(entityType) + "Corrected:" + str(anomalyTypeId) + ":" + id,
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
    # print(body)
    aquaspice_utils.upsert_context_broker(body)


def calculate_date_distance(last_observedAt_received, last_date_received, subscriptionId, station_id, current_date):
    '''
    Calculate distance between dates
    '''
    try:
        last_date = last_observedAt_received[subscriptionId][station_id]

        format = "%Y-%m-%dT%H:%M:%SZ"

        difference = datetime.strptime(current_date, format) - datetime.strptime(last_date, format)

        minutes = int(difference.total_seconds() / 60)

        print(f"### Debug date: current_date: {current_date} last date received {last_date}, diff: {minutes} minutes", flush=True)

        # If it has a data gap greater than {x} minutes, order a reset in the in-memory dicts
        if minutes >= 240:
            # Do something, reset in memory dicts?
            return True
        else:
            return False

    except Exception as e:
        print(f"Exception on calculating date distance: {e}", flush = True)
        pass

def get_datetime_now():
    """
    Return current date with correct format
    """
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

def logMessage(message, kind = "info"):
    '''
    Implement logger if necessary
    '''

    print('[' + str(datetime.now()) + '] ' + str(message), flush = True)

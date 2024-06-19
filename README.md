# AquaSPICE

The aim of AquaSPICE (Advancing Sustainability of Process Industries through Digital and Circular Water Use Innovations) is the adoption of circular water use practices in the industrial sector, the integration, and the demonstration of innovative solutions concerning the process, the resource-efficiency, and the digital tools. It aims at materializing circular water use in European Process Industries, fostering awareness in resource-efficiency and delivering compact solutions for industrial applications.

## RTM platform

The RTM platform focuses on real-time monitoring and operational modelling. It is integrated within the AquaSPICE ecosystem and it is based on the [FIWARE initiative](https://www.fiware.org/).

## DataQA

To guarantee that data ingested by the RTM platform is valid and can be confidently used by end users through dashboards, as well as by intelligent services to reason over it, the context broker [Orion](https://github.com/FIWARE/context.Orion-LD) has in its core the Data Quality Assurance module (DataQA). This module evaluates the input data (coming from on-site sensors) in real time and produces transformed time series with quality metrics associated to the original readings (e.g., flag invalid readings). This evaluation is implemented using a bag of algorithms and techniques offered by the DataQA module including feature engineering and outliers’ detection and correction.

The DataQA module analyzes input sample data for multiple entities as needed for the Case Study. 
During RTM deployment, a configuration file specifies each analysis process (as specified below), including its ID, algorithms to be used alongside with its hyperparameters, and target entities and properties. 
The data structure is also detailed in details below. This configuration makes the DataQA module generalizable and flexible to accept and work with different ```<<entityType>>``` and ```<<Property>>```.

## Fiware

FIWARE Foundation drives the definition – and the Open Source implementation – of key open standards that enable the development of portable and interoperable smart solutions in a faster, easier and affordable way, avoiding vendor lock-in scenarios, whilst also nurturing FIWARE as a sustainable and innovation-driven business ecosystem. The FIWARE platform provides a rather simple yet powerful set of APIs (Application Programming Interfaces) that ease the development of Smart Applications in multiple vertical sectors. It includes Core Context Broker components, Core Data Connectors, Context Processing, Analysis and Visualization, IoT agent interfaces and more.

## Orion

The Orion Context Broker currently provides the FIWARE NGSI v2 API which is a simple yet powerful Restful API enabling to perform updates, queries, or subscribe to changes on context information. The context broker (Orion-LD) is responsible for managing the lifecycle of context information: entities and their attributes. Using the APIs provided by the context broker it allows to create context elements, manage them through updates, update their attributes, perform queries to retrieve their status, and subscribe to context changes.

A Context Broker component is the core and mandatory component of any “Powered by FIWARE” platform or solution. It enables to manage context information in a highly decentralized and large-scale manner.

## Data models

The DataQA module is prepared to work with the following data models.

### Input data model

The input model is received through the subscription created on the Context Broker, as long as it matches the defined ```<<entityType>>``` and ```<<properties>>``` used while creating the subscription.

```json
{
    "$schema": "http://json-schema.org/schema#",
    "$schemaVersion": "0.0.1",
    "$id": "url:<<schema>>:<<entityType>>",
    "title": "AquaSpice Models - <<entityType>> Schema",
    "description": "<<entityType>> information definition",
    "type": "object",
    "allOf": [
        {
            "properties": {
              "type": {
                "type": "string",
                "enum": [
                  "<<entityType>>"
                ],
                "description": "Property. NGSI Entity type"
              },
              "<<propertyName>>": {
                "type": "Number",
                "description": "Property, Model:'https://schema.org/Number'"
              }
            }  
        }
    ],
    "required": [
        "type",
        "id",
        "<<propertyName>>"
    ]
}

```

### Output data model

For each message received by the DataQA module, the module will run the analyses defined in the configuration file and flag the value as correct or an outlier. 
As a response, a new JSON-LD message will then be sent back to the **RTM platform context broker**. 
This message will have an ```id``` of ```urn:ngsi-ld:AquaSpice:<<EntityType>>Corrected:<<EntityId>>``` and a type of ```<<EntityType>>Corrected```, where ```<<EntityType>>``` is defined in the configuration file and ```<<EntityId>>``` is extracted from the original message. 
The outgoing message will include all ```<<notCorrectedProperties>>``` from the original message and, for each of the variables in the ```<<analyzedProperty>>``` list, it will include the raw and corrected values. If the value is flagged as anomalous, the corrected value will be the moving average; otherwise, it will be the same as the raw value.
This way, the response given by the Data-QA can be ingested by the Context-Broker and due to it being generic and parameter-based it can be applied to any case of study.
```json
{
  "id": "urn:ngsi-ld:AquaSpice:<<entityType>><<flag>>:<<id>>",
  "type":  "<<entityType>><<flag>>",
  "<<PropertyName>>Raw":{
    "type": "Property", 
    "value": "The original value", 
    "observedAt": "Original observation time"
   },
  "<<PropertyName>>Corrected":{
     "type": "Property", 
     "value": "The corrected value", 
     "observedAt": "Original observation time"
   }
}
```

The parameter ```<<flag>>``` can be ```Corrected``` if the data sample was treated by the one of the outlier detection algorithms, or ```Flagged``` if it was validated through the WaterCPS water quality thresholds.


### Anomaly data model

The anomaly model is dedicated to representing anomalies found by the DataQA module while analyzing the received data samples in case of abnormal values.

```json
{
    "id": "urn:ngsi-ld:<<entityType>>Corrected",
    "type": "Anomaly",
    "name": "value-anomaly",
    "description": "Anomaly for subject: `<<subject>>`",
    "dateObserved": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"}
    },
    "validFrom": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"}
    },
    "validTo": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"}
    },
    "dateCreated": {
        "type": "Property",
        "value": {"type": "DateTime", "value": "date"}
    },
    "dateIssued": {
        "type": "Property",
        "value": {"type": "DateTime", "value": "date"}
    },
    "@context": [
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"                
    ]
}
```


# DataQA code explanation

General behaviour of the data-qa module is as follows:

Each data samples goes through a defined set of outlier detection algorithms when they are ingested by the DataQA Module. More specifically, the data can pass through one of two different approaches that are defined: 
 
- Approach #1 (Z-score method): Z-score is a statistical score method that evaluates a data point based on the standard deviation of what it is being observed against, in our case, a sample in a time-series is being compared to the standard value of values of this time series, in a pre-defined window of time. The z-score is then obtained by subtracting the time series mean from the individual sample score and then dividing the difference by the time series standard deviation, if the obtained value falls above 3 or below 0 in our case, it is considered an outlier. Moreover, this method is double-checked by the IQR threshold algorithm, which is also an statistical technique but based on the interquartile range, which measures the spread of the middle (approximately) of the dataset. Additionally, the IQR method is a robust technique when dealing with extreme values, which can improve the overall performance of the outlier detection. 
 
- Approach #2 (Hampel Filter): Hampel filter is a method used for identifying and handling outliers, which is particularly useful for time series data. It works by comparing each point of the series to the median of its nearby values (or neighbors) within a pre-defined window, and it is also robust to extreme values and resistant with non-normal distributions. Its parameters can be tuned by setting a window size and an n_sigma (Pearson's rule) which controls the tolerance of the algorithm. In this case, the Hampel Filter contains a weakness which is caused by the fact that it relies on the neighbors of the data point, which is that it doesn't work very well with the end of time series that may cause false positives while analyzing the data points. Because of that, this algorithm also uses the IQR method as a failsafe method to correctly label outlier values. 
 
- Approach #3 (WaterCPS method): This method is based on some thresholds defined in the Aquaspice project for the case study #3.1. There are three kinds of thresholds that help indicates if a sensor is providing successive abnormal values, which are the maximum, minimum and difference values based on a 1-hour period. Details are explained below. 

Finally, if a sample is labeled as an outlier, its value is then replaced by the moving average value of the series by the DataQA.

In short, this is the behaviour of the DataQA module:
- In-data memory objects are updated depending on the algorithm chosen (once), by querying historical data for each entity when a new entity sample is received.

- During the process reading, only the supplied entities and properties will be analyzed (as indicated in the data_qa_config file), after each data sample is analyzed, they are included in the in-memory objects.

- Samples are analyzed using the techniques defined in the data_qa_config file, and then if a sample is flagged as an outlier, it will generate an Anomaly but also replies with a corrected value.

Furthermore, the module has been developed in such a way that it can easily accommodate new algorithms. 

## Files explanation

- Folder config/: Includes configurations regarding the context broker address, and data_qa parameters for each of the algorithms, such as sliding window, query points, etc.

- File streaming_analysis.py: Contains all functions related to the analysis, the main function is the process_reading() which receives a new sample data and decides whether or not it is considered an outlier and produces a corrected reading.

- File context_broker_client_utils.py: Contains auxiliar functions related to the communication with the context broker.

- hampel_functions: Contains functions related to the Hampel filter outlier detection method.

- z_score_functions: Contains functions related to the Z-score outliers functions.

- watercps_functions: Contains functions related to the WaterCPS error flagging thresholds.

- auxiliar_functions: Contains additional functions used by the whole process.

## Configuration parameters explanation

- File config/data_qa_params.json is responsible for setting parameters of the dataQA algorithms.

| Parameter                      | Explanation                                                                                                            | Default value                    |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------|----------------------------------|
| query_points                   | Define the number of samples to query the context broker to build the historic dataset.                                | 4000                             |
| data_cadency_anomaly_threshold | Parameter which controls the distance required to trigger the data cadency anomaly detection (minutes).                | 1440                             |
| z_score_threshold              | Pearson's rule threshold used for Z-score.                                                                             | 4                                |
| hampel_filter_threshold        | Pearson's rule threshold used for Hampel filter module.                                                                | 5                                |
| property_sliding_window        | Indicates the size of the sliding window used for Hampel filter, the values are defined for each type of {{Property}}. | 192, 180, 180                    |
| iqr_threshold                  | Indicates the threshold used by the IQR method (failsafe)                                                              | 2, 1.8                           |
| watercps_error_flagging        | Contains the values used as thresholds for maximum, minimum and delta value                                            | Varies per variable and use case |


- File config/data_qa_config.json creates subscriptions that defines the analysis to be executed. It Defines ```<<entityType>>``` to be included in the analysis and as well  ```<<Property>>``` as ```<<analyzedProperties>>```, also defines the corresponding ```<<algorithm>>``` to be used. An example can be seen below.
```json
{"analysis" : 
    [
        {
            "algorithm" : "hampel_filter", 
            "subscription_id" : "urn:ngsi-ld:Subscription:{subscription_name}}",
            "entityType" : "MeasurementStation",
            "analyzedProperties" : ["temperature", "depth"],
            "anomalyTypeId" : "_anomaly_hampel_filter",
            "notCorrectedProperties" : ["location"]
        },
        {
            "algorithm" : "z_score", 
            "subscription_id" : "urn:ngsi-ld:Subscription:{subscription_name}",
            "entityType" : "MeasurementStation",
            "analyzedProperties" : ["conductivity"],
            "anomalyTypeId" : "_anomaly_z_score",
            "notCorrectedProperties" : ["location"]
        },
        {
            "algorithm" : "watercps_threshold",
                "subscription_id" : "urn:ngsi-ld:Subscription:{subscription_name}",
            "entityType" : "MeasurementStation",
            "analyzedProperties" : ["temperature", "depth", "conductivity"],
            "anomalyTypeId" : "_anomaly_watercps",
            "notCorrectedProperties" : ["location"]
        }
    ]
}
```

- File config/base_config.json includes parameters (such as URL's) of the context broker.

## Usage


```
docker build -t data_q_module .
```

# Dependencies

Required libraries are listed under requirements.txt file.

# Implemented algorithms and outlier detection methods

## Z-score outlier detection

Z-Score outlier detection method is implemented. Pearson parameter = 4.


## Hampel Filter outlier detection

Hampel Filter outlier detection implemented. Specific variables have different sliding windows, pearson parameter = 4.

## IQR Method.

The statistical method is implemented as an failsafe The default values are 1.8 or 2 depending on the case.

## WaterCPS thresholds for error flagging

The aim of these thresholds is to act as an indicative to detect the need of maintenance of the sensors defined for the Case study #3.1. Basically, it relies on 3 different thresholds defined for each variable:

- Minimum criterion: Minimum value for average of measurements within 1 hour.
- Maximum criterion: Maximum value for average of measurements within 1 hour.
- Jump criterion: Difference between 2 successive measurements (within 1 hour of interval)/

# Contributors

The current mantainers of the project are:

- [Danillo Lange](https://github.com/roxdan/)

Copyright 2018 Eurecat

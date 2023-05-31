# AquaSPICE

The aim of AquaSPICE (Advancing Sustainability of Process Industries through Digital and Circular Water Use Innovations) is the adoption of circular water use practices in the industrial sector, the integration, and the demonstration of innovative solutions concerning the process, the resource-efficiency, and the digital tools. It aims at materializing circular water use in European Process Industries, fostering awareness in resource-efficiency and delivering compact solutions for industrial applications.

## RTM platform

The RTM platform focuses on real-time monitoring and operational modelling. It is integrated within the AquaSPICE ecosystem and it is based on the [FIWARE initiative](https://www.fiware.org/).

To guarantee that data ingested by the RTM platform is valid and can be confidently used by end users through dashboards, as well as by intelligent services to reason over it, the context broker [Orion](https://github.com/FIWARE/context.Orion-LD) has in its core the Data Quality Assurance module (DataQA). This module evaluates the input data (coming from on-site sensors) in real time and produces transformed time series with quality metrics associated to the original readings (e.g., flag invalid readings). This evaluation is implemented using a bag of algorithms and techniques offered by the DataQA module including feature engineering and outliers’ detection and correction.

## Fiware

FIWARE Foundation drives the definition – and the Open Source implementation – of key open standards that enable the development of portable and interoperable smart solutions in a faster, easier and affordable way, avoiding vendor lock-in scenarios, whilst also nurturing FIWARE as a sustainable and innovation-driven business ecosystem. The FIWARE platform provides a rather simple yet powerful set of APIs (Application Programming Interfaces) that ease the development of Smart Applications in multiple vertical sectors. It includes Core Context Broker components, Core Data Connectors, Context Processing, Analysis and Visualization, IoT agent interfaces and more.

## Orion

The Orion Context Broker currently provides the FIWARE NGSI v2 API which is a simple yet powerful Restful API enabling to perform updates, queries, or subscribe to changes on context information. The context broker (Orion-LD) is responsible for managing the lifecycle of context information: entities and their attributes. Using the APIs provided by the context broker it allows to create context elements, manage them through updates, update their attributes, perform queries to retrieve their status, and subscribe to context changes.

A Context Broker component is the core and mandatory component of any “Powered by FIWARE” platform or solution. It enables to manage context information in a highly decentralized and large-scale manner.

## Data models

The data-qa module is prepared to work with the following data models:

### Input data model

```json
{
    "$schema": "http://json-schema.org/schema#",
    "$schemaVersion": "0.0.1",
    "$id": "url:schema:{{entityType}}",
    "title": "AquaSpice Models - {{entityType}} Schema",
    "description": "{{entityType}} information definition",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "type": {
                    "type": "string",
                    "enum": [
                        "{{entityType}}"
                    ],
                    "description": "Property. NGSI Entity type"
                },
                "temperature": {
                    "type": "Number",
                    "description": "Property, Temperature of the water, Model:'https://schema.org/Number'"
                },
                "depth": {
                    "type": "Number",
                    "$ref": "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/depth"
                },
                "location": {
                    "$ref": "https://smart-data-models.github.io/data-models/common-schema.json#/definitions/Location-Commons/properties/location"
                },
                "conductivity": {
                    "type": "Number",
                    "$ref": "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/conductivity"
                }
            }
        }
    ],
    "required": [
        "type",
        "id",
        "temperature",
        "depth",
        "location",
        "conductivity"
    ]
}

```

### Output data model

Schema of the response given by the data-qa module.

```json
{
    "$schema": "http://json-schema.org/schema#",
    "$schemaVersion": "0.0.1",
    "$id": "url:schema:{{entityType}}Corrected",
    "title": "AquaSpice Models - {{entityType}}Corrected Schema",
    "description": "{{entityType}}Corrected information definition",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "type": {
                    "type": "string",
                    "enum": [
                        "{{entityType}}"
                    ],
                    "description": "Property. NGSI Entity type"
                },
                "temperatureRaw": {
                    "type": "Number",
                    "description": "Property, Temperature of the water, Model:'https://schema.org/Number'"
                },
                "temperatureCorrected": {
                    "type": "Number"
                },
                "depthRaw": {
                    "type": "Number",
                    "$ref": "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/depth"
                },
                "depthCorrected": {
                    "type": "Number"
                },
                "conductivityRaw": {
                    "type": "Number",
                    "$ref": "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/conductivity"
                },
                "conductivityCorrected": {
                    "type": "Number"
                }

            }
        }
    ]
}
```

### Anomaly data model

The anomaly model is dedicated to representing anomalies found by the data-qa module while analyzing the received data samples in case of abnormal values.

```json
{
    "id": "urn:ngsi-ld:{{entityType}}Corrected",
    "type": "Anomaly",
    "name": "value-anomaly",
    "description": "Anomaly for subject: {{subject}}",
    "dateObserved": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"},
    },
    "validFrom": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"},
    },
    "validTo": {
        "type": "Property",
        "value": {"@type": "DateTime", "@value": "date"},
    },
    "dateCreated": {
        "type": "Property",
        "value": {"type": "DateTime", "value": "date"()},
    },
    "dateIssued": {
        "type": "Property",
        "value": {"type": "DateTime", "value": "date"()},
    },
    "@context": [
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"                
    ]
}
```


# DataQA code explanation

General behaviour of the data-qa module is as follows:

- In-data memory objects are updated depending on the algorithm chosen (once), by querying historical data for each entity when a new entity sample is received.

- During the process reading, only the supplied entities and properties will be analyzed (as indicated in the data_qa_config file), after each data sample is analyzed, they are included in the in-memory objects.

- Samples are analyzed using the techniques defined in the data_qa_config file, and then if a sample is flagged as an outlier, it will generate an Anomaly but also replies with a corrected value.

## Files explanation

- Folder config/: Includes configurations regarding the context broker address, and data_qa parameters for each of the algorithms, such as sliding window, query points, etc.

- File streaming_analysis.py: Contains all functions related to the analysis, the main function is the process_reading() which receives a new sample data and decides whether or not it is considered an outlier and produces a corrected reading.

- File context_broker_client_utils.py: Contains auxiliar functions related to the communication with the context broker.

## Configuration parameters explanation

- File config/data_qa_params.json is responsible for setting parameters of the dataQA algorithms.

| Parameter| Explanation | Default value |
|---|---|---|
| query_points  | Define the number of samples to query the context broker to build the historic dataset.  | 4000  |
| data_cadency_anomaly_threshold  | Parameter which controls the distance required to trigger the data cadency anomaly detection (minutes).  | 1440   |
| z_score_threshold  | Pearson's rule threshold used for Z-score.  | 4  |
| hampel_filter_threshold  | Pearson's rule threshold used for Hampel filter module.  | 5 |
| property_sliding_window  | Indicates the size of the sliding window used for Hampel filter, the values are defined for each type of {{Property}}.  | 192, 180, 180  |

- File config/data_qa_config.json creates subscriptions that defines the analysis to be executed. Defines {{entityType}} to be included in the analysis and as well  {{Property}} as {{analyzedProperties}}, also defines the corresponding {{algorithm}} to be used.

- File config/base_config.json includes parameters of the context broker.

## Usage

```
python -u ./src/streaming_analysis.py "config/base_config.json;config/data_qa_params.json;config/data_qa_config.json"
```

# Dependencies

Required libraries are listed under requirements.txt file.

## Z-score outlier detection

Z-Score outlier detection method is implemented. Pearson parameter = 4.


## Hampel Filter outlier detection

Hampel Filter outlier detection implemented. Specific variables have different sliding windows, pearson parameter = 4.

## Future releases

Future releases will include additional anomaly detection algorithms and improvements on the existing ones.


# Contributors

The current mantainers of the project are:

- [Danillo Lange](https://github.com/roxdan/)

Copyright 2018 Eurecat
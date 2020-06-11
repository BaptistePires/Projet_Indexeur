# Indexer

Indexer project as part of the **LU3IN107 - WEB Technology** teaching unit.\
Supervised by [Olivier PITTON](https://www.linkedin.com/in/olivier-pitton-42604960/)\
[Sorbonne University](http://www.sorbonne-universite.fr/)

## Contributors

- [Francis MURRAY](mailto:franciswmurray@gmail.com)
- [Massil TAGUEMOUT](mailto:massitaguemout@gmail.com)
- [Baptiste PIRES](mailto:baptiste.pires37@gmail.com)

## Project description:

The goal of this project is to conceive and develop an Indexing Engine facilitating fast and accurate information retrieval from a database.

## Data Source:

NYC TLC Trip Record Data: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page


## Documentation

- Swagger API Documentation: http://localhost:8080/
- OpenAPI Specification: http://localhost:8080/api/openapi.json
- Project Information and Reports: Execute `mvn site:run` in a separate terminal. The reports will be generated and visible at http://localhost:9000/

## Getting Started

- To run the project, execute `mvn jetty:run-war`

## Operations:

- Create Table
  - API Call (POST): http://localhost:8080/api/indexer/createTable
  * Request Body:
    ```json
    {
      "name": "TableName",
      "columns": [
        {
          "name": "VendorID",
          "type": "Integer"
        },
        {
          "name": "tpep_pickup_datetime",
          "type": "String"
        },
        {
          "name": "tpep_dropoff_datetime",
          "type": "String"
        },
        {
          "name": "passenger_count",
          "type": "Integer"
        },
        {
          "name": "trip_distance",
          "type": "Double"
        },
        {
          "name": "RatecodeID",
          "type": "Integer"
        },
        {
          "name": "store_and_fwd_flag",
          "type": "String"
        },
        {
          "name": "PULocationID",
          "type": "Integer"
        },
        {
          "name": "DOLocationID",
          "type": "Integer"
        },
        {
          "name": "payment_type",
          "type": "Integer"
        },
        {
          "name": "fare_amount",
          "type": "Double"
        },
        {
          "name": "extra",
          "type": "Double"
        },
        {
          "name": "mta_tax",
          "type": "Double"
        },
        {
          "name": "tip_amount",
          "type": "Double"
        },
        {
          "name": "tolls_amount",
          "type": "Double"
        },
        {
          "name": "improvement_surcharge",
          "type": "Double"
        },
        {
          "name": "total_amount",
          "type": "Double"
        },
        {
          "name": "congestion_surcharge",
          "type": "Integer"
        }
      ]
    }
    ```

- Add Indexes
  - API Call (POST): http://localhost:8080/api/indexer/addIndexes
  * Request Body example:
    ```json
    {
      "tableName": "TableName",
      "indexes": [
          "VendorID", 
          "passenger_count"

        ]
    }
    ```

- Upload data
  - API Call (POST): http://localhost:8080/api/indexer/uploadData
  * Request Body: 
    * form-data
    *   file = yellow_tripdata_2019-01.csv (csv files can be downloaded here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

- Start Indexing
  - API Call (POST): http://localhost:8080/api/indexer/startIndexing
  * Request Body: none

- Query
  - API Call (POST): http://localhost:8080/api/indexer/query
  * Request Body example:
    ```json
    {
      "type": "SELECT",
      "cols": ["VendorID", "passenger_count"],
      "from": "TableName",
      "operator": "AND",
      "where": {
        "VendorID": {
          "operator": "=",
          "value": 2
        },
        "fare_amount": {
          "operator": "=",
          "value": 5
        }
      },
      "limit": 5000
    }
    ```

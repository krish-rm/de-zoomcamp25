

```bash
docker ps
```

Go to local directory of Docker-compose and Csv Files

```bash
docker-compose up -d
```

Once pgadmin and postgres containers are Running, then Create table on ny_taxi DB for green_taxi_trips and trip_zones on postgres db in container.

1. First connect to postgres container through psql

```bash
docker exec -it postgres psql -U postgres -d ny_taxi
```
2. Create tables for green_taxi_trips and trip_zones

```sql
CREATE TABLE "green_taxi_trips" (
"VendorID" INTEGER,
  "lpep_pickup_datetime" TIMESTAMP,
  "lpep_dropoff_datetime" TIMESTAMP,
  "store_and_fwd_flag" TEXT,
  "RatecodeID" INTEGER,
  "PULocationID" INTEGER,
  "DOLocationID" INTEGER,
  "passenger_count" INTEGER,
  "trip_distance" REAL,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "ehail_fee" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL,
  "payment_type" INTEGER,
  "trip_type" INTEGER,
  "congestion_surcharge" REAL
);

CREATE TABLE "taxi_zones" (
  "LocationID" INTEGER PRIMARY KEY,
  "Borough" TEXT,
  "Zone" TEXT,
  "service_zone" TEXT
);
```

3. Check if tables are created

```
\dt

\d green_taxi_trips
\d taxi_zones
```

4. Once tables are created, then time to copy csv data to postgres DB. For this first copy csv data to /tmp in postgres container

```bash
docker cp C:\Users\lenovo\PRACTICALS\Homework\module-1_homework\green_tripdata_2019-10.csv postgres:/tmp/green_tripdata_2019-10.csv
docker cp C:\Users\lenovo\PRACTICALS\Homework\module-1_homework\taxi_zone_lookup.csv postgres:/tmp/taxi_zone_lookup.csv
```

5. Connect to psql again and copy csv data from /tmp to postgres DB

```bash
docker exec -it postgres psql -U postgres -d ny_taxi
```

```sql
COPY green_taxi_trips(
    "VendorID", 
    "lpep_pickup_datetime", 
    "lpep_dropoff_datetime", 
    "store_and_fwd_flag", 
    "RatecodeID", 
    "PULocationID", 
    "DOLocationID", 
    "passenger_count", 
    "trip_distance", 
    "fare_amount", 
    "extra", 
    "mta_tax", 
    "tip_amount", 
    "tolls_amount", 
    "ehail_fee", 
    "improvement_surcharge", 
    "total_amount", 
    "payment_type", 
    "trip_type", 
    "congestion_surcharge"
)
FROM '/tmp/green_tripdata_2019-10.csv'
DELIMITER ',' 
CSV HEADER;
```
```sql
COPY "taxi_zones"("LocationID", "Borough", "Zone", "service_zone")
FROM '/tmp/taxi_zone_lookup.csv'
DELIMITER ','
CSV HEADER;
```

6. Check the data upload on pgadmin ny_taxi DB. There should be green_taxi_trips and taxi_zones with data succefully uploaded.


### How to get schema to create Tables using pandas.

```Python
import pandas as pd
df = pd.read_csv("green_tripdata_2019-10.csv", nrows=1000)
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
print(pd.io.sql.get_schema(df, name='green_taxi_trips'))
```

### How to use Jupyter or python to connect to postgres container

```Python
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()
```
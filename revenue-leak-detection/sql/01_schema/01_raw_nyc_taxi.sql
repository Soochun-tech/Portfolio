USE portfolio;

DROP TABLE IF EXISTS raw_yellow_taxi;
CREATE TABLE raw_yellow_taxi (
    vendorid              TINYINT,
    tpep_pickup_datetime  DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count       DECIMAL(4,1),
    trip_distance         DECIMAL(10,3),
    ratecodeid            DECIMAL(4,1),
    store_and_fwd_flag    CHAR(1),
    pulocationid          SMALLINT,
    dolocationid          SMALLINT,
    payment_type          TINYINT,
    fare_amount           DECIMAL(10,2),
    extra                 DECIMAL(10,2),
    mta_tax               DECIMAL(10,2),
    tip_amount            DECIMAL(10,2),
    tolls_amount          DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount          DECIMAL(10,2),
    congestion_surcharge  DECIMAL(10,2),
    airport_fee           DECIMAL(10,2),

    KEY idx_pickup (tpep_pickup_datetime),
    KEY idx_pu_zone_dt (pulocationid, tpep_pickup_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_taxi_zone_lookup;
CREATE TABLE raw_taxi_zone_lookup (
    locationid    SMALLINT      PRIMARY KEY,
    borough       VARCHAR(50),
    zone          VARCHAR(100),
    service_zone  VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

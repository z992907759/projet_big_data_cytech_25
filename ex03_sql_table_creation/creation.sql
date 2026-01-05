-- ex03_sql_table_creation/creation.sql
-- EX3: Data Warehouse schema (star schema)

CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimensions
CREATE TABLE IF NOT EXISTS dwh.dim_vendor (
                                              vendor_key   SERIAL PRIMARY KEY,
                                              vendor_id    INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_rate_code (
                                                 rate_code_key SERIAL PRIMARY KEY,
                                                 rate_code_id  INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_payment_type (
                                                    payment_type_key SERIAL PRIMARY KEY,
                                                    payment_type_id  INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_location (
                                                location_key SERIAL PRIMARY KEY,
                                                location_id  INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_datetime (
                                                datetime_key SERIAL PRIMARY KEY,
                                                ts           TIMESTAMP UNIQUE NOT NULL,
                                                date         DATE NOT NULL,
                                                year         INT NOT NULL,
                                                month        INT NOT NULL,
                                                day          INT NOT NULL,
                                                hour         INT NOT NULL,
                                                dow          INT NOT NULL
);

-- Fact table: 1 row = 1 trip
CREATE TABLE IF NOT EXISTS dwh.fact_trip (
                                             trip_key SERIAL PRIMARY KEY,

                                             pickup_datetime_key  INT NOT NULL,
                                             dropoff_datetime_key INT NOT NULL,

                                             vendor_key       INT,
                                             rate_code_key    INT,
                                             payment_type_key INT,

                                             pu_location_key  INT,
                                             do_location_key  INT,

                                             passenger_count  INT,
                                             trip_distance    DOUBLE PRECISION,

                                             fare_amount      DOUBLE PRECISION,
                                             extra            DOUBLE PRECISION,
                                             mta_tax          DOUBLE PRECISION,
                                             tip_amount       DOUBLE PRECISION,
                                             tolls_amount     DOUBLE PRECISION,
                                             improvement_surcharge DOUBLE PRECISION,
                                             total_amount     DOUBLE PRECISION,

                                             source_file TEXT,

                                             CONSTRAINT fk_pickup_datetime
                                             FOREIGN KEY (pickup_datetime_key) REFERENCES dwh.dim_datetime(datetime_key),

    CONSTRAINT fk_dropoff_datetime
    FOREIGN KEY (dropoff_datetime_key) REFERENCES dwh.dim_datetime(datetime_key),

    CONSTRAINT fk_vendor
    FOREIGN KEY (vendor_key) REFERENCES dwh.dim_vendor(vendor_key),

    CONSTRAINT fk_rate_code
    FOREIGN KEY (rate_code_key) REFERENCES dwh.dim_rate_code(rate_code_key),

    CONSTRAINT fk_payment_type
    FOREIGN KEY (payment_type_key) REFERENCES dwh.dim_payment_type(payment_type_key),

    CONSTRAINT fk_pu_location
    FOREIGN KEY (pu_location_key) REFERENCES dwh.dim_location(location_key),

    CONSTRAINT fk_do_location
    FOREIGN KEY (do_location_key) REFERENCES dwh.dim_location(location_key)
    );

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_dt ON dwh.fact_trip(pickup_datetime_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_pu_loc    ON dwh.fact_trip(pu_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_do_loc    ON dwh.fact_trip(do_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_vendor    ON dwh.fact_trip(vendor_key);
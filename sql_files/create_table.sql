-- Create schema to store data
create schema aiq_data;
-- Create staging area
create schema aiq_data_staging;

----- Create Sales Order Table ---------
DROP TABLE IF EXISTS aiq_data.sales_order;
CREATE TABLE aiq_data.sales_order (
    order_id integer NOT NULL ENCODE az64,
    customer_id integer ENCODE az64,
    product_id character varying(100) ENCODE lzo,
    quantity numeric(18,0) ENCODE az64,
    price numeric(12,2) ENCODE az64,
    order_date timestamp without time zone ENCODE az64
)
DISTSTYLE AUTO
SORTKEY ( order_date );

----- Create Customer Table ---------
DROP TABLE IF EXISTS aiq_data.customer_details;
CREATE TABLE aiq_data.customer_details (
    id integer NOT NULL ENCODE az64,
    name character varying(255) ENCODE lzo,
    username character varying(255) ENCODE lzo,
    email character varying(255) ENCODE lzo,
    phone character varying(255) ENCODE lzo,
    suit character varying(100) ENCODE lzo,
    city character varying(100) ENCODE lzo,
    zipcode character varying(100) ENCODE lzo,
    lat numeric(18,4) ENCODE az64,
    lon numeric(18,4) ENCODE az64,
    PRIMARY KEY (id)
)
DISTSTYLE AUTO
SORTKEY ( id );

----- Create Weather Table ---------
DROP TABLE IF EXISTS aiq_data.weather_details;
CREATE TABLE aiq_data.weather_details (
    weather_id integer ENCODE az64,
    weather_type character varying(55) ENCODE lzo,
    description character varying(255) ENCODE lzo,
    base character varying(100) ENCODE lzo,
    temp numeric(18,2) ENCODE az64,
    feel_like numeric(18,2) ENCODE az64,
    min_temp numeric(18,2) ENCODE az64,
    max_temp numeric(18,2) ENCODE az64,
    pressure numeric(18,0) ENCODE az64,
    humidity numeric(18,0) ENCODE az64,
    visibility numeric(18,0) ENCODE az64,
    wind_speed numeric(18,2) ENCODE az64,
    wind_deg numeric(18,0) ENCODE az64,
    wind_gust numeric(18,2) ENCODE az64,
    cloud_all numeric(18,0) ENCODE az64,
    date timestamp without time zone NOT NULL ENCODE az64,
    lat numeric(18,4) NOT NULL ENCODE az64 ,
    lon numeric(18,4) NOT NULL ENCODE az64,
    PRIMARY KEY (date, lat, lon)
)
DISTSTYLE AUTO
SORTKEY ( date );

----- Create Sales Order Staging Table ---------
DROP TABLE IF EXISTS aiq_data_staging.sales_order;
CREATE TABLE aiq_data_staging.sales_order (
    order_id integer ENCODE az64,
    customer_id integer ENCODE az64,
    product_id character varying(100) ENCODE lzo,
    quantity numeric(18,0) ENCODE az64,
    price numeric(12,2) ENCODE az64,
    order_date timestamp without time zone ENCODE az64
)
DISTSTYLE AUTO
SORTKEY ( order_date );

----- Create Customer Staging Table ---------
DROP TABLE IF EXISTS aiq_data_staging.customer_details;
CREATE TABLE aiq_data_staging.customer_details (
    id integer ENCODE az64,
    name character varying(255) ENCODE lzo,
    username character varying(255) ENCODE lzo,
    email character varying(255) ENCODE lzo,
    phone character varying(255) ENCODE lzo,
    suit character varying(100) ENCODE lzo,
    city character varying(100) ENCODE lzo,
    zipcode character varying(100) ENCODE lzo,
    lat numeric(18,4) ENCODE az64,
    lon numeric(18,4) ENCODE az64
)
DISTSTYLE AUTO
SORTKEY ( id );

----- Create Weather Staging Table ---------
DROP TABLE IF EXISTS aiq_data_staging.weather_details;
CREATE TABLE aiq_data_staging.weather_details (
    weather_id integer ENCODE az64,
    weather_type character varying(55) ENCODE lzo,
    description character varying(255) ENCODE lzo,
    base character varying(100) ENCODE lzo,
    temp numeric(18,2) ENCODE az64,
    feel_like numeric(18,2) ENCODE az64,
    min_temp numeric(18,2) ENCODE az64,
    max_temp numeric(18,2) ENCODE az64,
    pressure numeric(18,0) ENCODE az64,
    humidity numeric(18,0) ENCODE az64,
    visibility numeric(18,0) ENCODE az64,
    wind_speed numeric(18,2) ENCODE az64,
    wind_deg numeric(18,0) ENCODE az64,
    wind_gust numeric(18,2) ENCODE az64,
    cloud_all numeric(18,0) ENCODE az64,
    date timestamp without time zone ENCODE az64,
    lat numeric(18,4) ENCODE az64,
    lon numeric(18,4) ENCODE az64
)
DISTSTYLE AUTO
SORTKEY ( date );

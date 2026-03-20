/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 1: Database and Schema Setup
=============================================================================
*/

-- Create the main database
CREATE DATABASE IF NOT EXISTS PON_EV_LAB
    COMMENT = 'Pon Automotive - EV Transition Netherlands Analytics';

USE DATABASE PON_EV_LAB;

-- Create schema layers (medallion architecture)
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data from RDW APIs - unchanged source data';

CREATE SCHEMA IF NOT EXISTS CURATED
    COMMENT = 'Curated data - cleaned, validated, and joined';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics layer - aggregations and business metrics';

-- Switch to RAW schema for table creation
USE SCHEMA RAW;

-- Registered vehicles (gekentekende voertuigen)
CREATE TABLE IF NOT EXISTS VEHICLES_RAW (
    kenteken STRING COMMENT 'License plate number (primary key)',
    datum_eerste_tenaamstelling_in_nederland STRING COMMENT 'First registration date in NL (YYYYMMDD)',
    merk STRING COMMENT 'Vehicle brand (e.g., VOLKSWAGEN, TESLA)',
    handelsbenaming STRING COMMENT 'Commercial model name',
    voertuigsoort STRING COMMENT 'Vehicle type',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Fuel types per vehicle
CREATE TABLE IF NOT EXISTS VEHICLES_FUEL_RAW (
    kenteken STRING COMMENT 'License plate number (foreign key)',
    brandstof_omschrijving STRING COMMENT 'Fuel type description',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Parking locations with charging
CREATE TABLE IF NOT EXISTS PARKING_ADDRESS_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    parkingaddresstype STRING COMMENT 'Type of address (F = facility)',
    zipcode STRING COMMENT 'Postal code',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Charging capacity per location
CREATE TABLE IF NOT EXISTS CHARGING_CAPACITY_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    chargingpointcapacity STRING COMMENT 'Number of charging points',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Vehicles by postal code (KEY dataset for regional EV analysis)
CREATE TABLE IF NOT EXISTS VEHICLES_BY_POSTCODE_RAW (
    postcode STRING COMMENT '4-digit postal code',
    voertuigsoort STRING COMMENT 'Vehicle type (Personenauto, Bedrijfsauto, etc.)',
    brandstof STRING COMMENT 'Fuel type: B=Benzine, D=Diesel, E=Electric',
    extern_oplaadbaar STRING COMMENT 'Plug-in capable: J/N',
    aantal INT COMMENT 'Number of vehicles',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Create the analytics warehouse (used by all subsequent modules)
CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Pon EV Analytics';

-- Verify setup
SHOW SCHEMAS IN DATABASE PON_EV_LAB;
SHOW TABLES IN SCHEMA PON_EV_LAB.RAW;

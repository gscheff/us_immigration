from datetime import date

from furl import furl
import awswrangler as wr
import sqlalchemy as sa

from . import base
from . import config


class FactImmigration(base.Table):

    def __init__(self, day: date, table, database, bucket: furl, prefix):
        self.day = day
        self.table = table
        self.database = database
        self.external_location = (bucket / prefix / table).url
        self.dim_country = config.DIM_COUNTRY_TABLE
        self.dim_port = config.DIM_PORT_TABLE

    @property
    def ddl(self):
        return f"""

    /* US immigration data */
    CREATE EXTERNAL TABLE IF NOT EXISTS {self.table}(
      cic_id bigint COMMENT '',
      admnum bigint COMMENT '',
      arrival_date date COMMENT '',
      departure_date date COMMENT '',
      port_code string COMMENT '',
      port_type string COMMENT '',
      airport_ident string COMMENT '',
      airport_type string COMMENT '',
      airport string COMMENT '',
      municipality string COMMENT '',
      city string COMMENT '',
      state_code string COMMENT 'iso code',
      airline string COMMENT '',
      flight_number string COMMENT '',
      visa string COMMENT '',
      birth_iso_country string COMMENT '',
      residence_iso_country string COMMENT '',
      birth_year int COMMENT '',
      gender varchar(6) COMMENT '')
    PARTITIONED BY (
      year string COMMENT '',
      month string COMMENT '',
      day string COMMENT '')
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '{self.external_location}'
    TBLPROPERTIES (
      'has_encrypted_data'='false',
      'parquet.compression'='SNAPPY')
    ;

        """

    @property
    def ctas(self):
        return f"""
    CREATE TABLE IF NOT EXISTS {self.table}
    WITH (
      external_location='{self.external_location}',
      format='PARQUET',
      parquet_compression='SNAPPY',
      partitioned_by=array['year', 'month', 'day']
    ) AS
    """

    @property
    def query(self):
        query = sa.text("""
WITH e_port AS (
    SELECT
      i94.cicid,
      if(i94port.code = 'XXX', NULL, i94port.code) AS port_code,
      if(lower(i94mode.name) LIKE 'not reported%', NULL, i94mode.name) AS port_type
    FROM {self.database}.i94
        LEFT join {self.database}.i94mode ON i94.i94mode = i94mode.code
        LEFT JOIN {self.database}.i94port ON I94.i94port = i94port.code
    WHERE year || month || day = :day)
-- fact_immigration
SELECT
    i94.cicid AS cic_id,
    i94.admnum,
    i94.datetime AS arrival_date,
    date_add('day', i94.depdate, date '1960-01-01') AS departure_date,
    port.port_code,
    port.port_type,
    airport.ident AS airport_ident,
    split_part(airport.type, '_', 1) AS airport_type,
    airport.name AS airport,
    airport.municipality,
    dim_port.city,
    dim_port.state_code,
    i94.airline,
    i94.fltno AS flight_number,
    i94visa.name AS visa,
    birth.iso_country AS birth_iso_country,
    residence.iso_country AS residence_iso_country,
    i94.biryear AS birth_year,
    CASE
      WHEN i94.gender = 'F' THEN 'female'
      WHEN i94.gender = 'M' THEN 'male'
    END AS gender,
    i94.year,
    i94.month,
    i94.day
FROM {self.database}.i94
    JOIN e_port AS port
        ON i94.cicid = port.cicid
    LEFT JOIN {self.database}.{self.dim_country} AS birth
        ON i94.i94cit = birth.i94country_code
    LEFT JOIN {self.database}.{self.dim_country} AS residence
        ON i94.i94res = residence.i94country_code
    LEFT JOIN {self.database}.i94visa
        ON i94.i94visa = i94visa.code
    LEFT JOIN {self.database}.{self.dim_port} AS dim_port
        ON port.port_code = dim_port.port_code
            AND port.port_type = dim_port.port_type
    LEFT JOIN {self.database}.airport
        ON dim_port.port_code = airport.iata_code
            AND dim_port.port_type = 'air'
            AND airport.iso_country = 'US'
WHERE year || month || day = :day
;
        """)
        query = query.bindparams(day=self.day.strftime('%Y%m%d'))
        options = dict(compile_kwargs={"literal_binds": True})
        query = str(query.compile(**options))
        return query

    @property
    def insert(self):
        return f"""
            INSERT INTO {self.table}
        """

    @property
    def drop(self):
        return f"""
            DROP TABLE {self.table}
        """

    @property
    def repair(self):
        return f"""
            MSCK REPAIR {self.table}
        """


class FactDemographics(base.Table):

    def __init__(self, table, database, bucket: furl, prefix):
        self.table = table
        self.database = database
        self.external_location = (bucket / prefix / table).url

    @property
    def ddl(self):
        return f"""

CREATE EXTERNAL TABLE {self.table}(
  city string COMMENT '',
  state_code string COMMENT '',
  state string COMMENT '',
  median_age double COMMENT '',
  male_population int COMMENT '',
  female_population int COMMENT '',
  total_population bigint COMMENT '',
  number_of_veterans int COMMENT '',
  foreign_born int COMMENT '',
  average_household_size double COMMENT '',
  asia_population bigint COMMENT '',
  black_population bigint COMMENT '',
  hispanic_population bigint COMMENT '',
  native_population bigint COMMENT '',
  white_population bigint COMMENT '')
CLUSTERED BY (
  city)
INTO 1 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{self.external_location}'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'parquet.compression'='SNAPPY')

        """

    @property
    def ctas(self):
        return f"""
    CREATE TABLE IF NOT EXISTS {self.table}
    WITH (
      external_location='{self.external_location}',
      format='PARQUET',
      parquet_compression='SNAPPY',
      bucketed_by=ARRAY['city'],
      bucket_count=1
    ) AS
        """

    @property
    def query(self):
        return f"""

    select
       city,
       state_code,
       state,
       max(median_age) as median_age,
       cast(max(male_population) as integer) as male_population,
       cast(max(female_population) as integer) as female_population,
       max(total_population) as total_population,
       cast(max(number_of_veterans) as integer) as number_of_veterans,
       cast(max(foreign_born) as integer) as foreign_born,
       round(max(average_household_size), 2) as average_household_size,
       max(if(race='Asian', count, null)) as asia_population,
       max(if(race='Black or African-American', count, null)) as black_population,
       max(if(race='Hispanic or Latino', count, null)) as hispanic_population,
       max(if(race='American Indian and Alaska Native', count, null)) as native_population,
       max(if(race='White', count, null)) as white_population
    from {self.database}.demographics
    group by 1,2,3;

        """

    @property
    def insert(self):
        return f"""
            INSERT INTO {self.table}
        """

    @property
    def drop(self):
        return f"""
            DROP TABLE {self.table}
        """

    @property
    def repair(self):
        return f"""
            MSCK REPAIR {self.table}
        """


class CreateFactImmigrationTable(base.BaseQueryExecution):

    def __init__(self, table, bucket, prefix, database):
        super().__init__(database)
        self.table = table
        self.external_location = (bucket / prefix / table).url

    @property
    def ddl(self):
        return f"""

    /* US immigration data */
    CREATE EXTERNAL TABLE IF NOT EXISTS {self.table}(
      cic_id bigint COMMENT '',
      admnum bigint COMMENT '',
      arrival_date date COMMENT '',
      departure_date date COMMENT '',
      port_code string COMMENT '',
      port_type string COMMENT '',
      airport_ident string COMMENT '',
      airport_type string COMMENT '',
      airport string COMMENT '',
      municipality string COMMENT '',
      city string COMMENT '',
      state_code string COMMENT 'iso code',
      airline string COMMENT '',
      flight_number string COMMENT '',
      visa string COMMENT '',
      birth_iso_country string COMMENT '',
      residence_iso_country string COMMENT '',
      birth_year int COMMENT '',
      gender varchar(6) COMMENT '')
    PARTITIONED BY (
      year string COMMENT '',
      month string COMMENT '',
      day string COMMENT '')
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '{self.external_location}'
    TBLPROPERTIES (
      'has_encrypted_data'='false',
      'parquet.compression'='SNAPPY')
    ;

        """

    @property
    def stmt(self):
        return self.ddl


class RepairFactImmigrationTable(base.BaseQueryExecution):

    stmt = "MSCK REPAIR TABLE fact_immigration;"


class InsertFactImmigrationTable(base.BaseQueryExecution):

    def __init__(self, day: date, table, database):
        super().__init__(database)
        self.day = day
        self.table = table

    @property
    def query(self):

        query = sa.text("""
with e_port as (
    select
      i94.cicid,
      if(i94port.code = 'XXX', null, i94port.code) as port_code,
      if(lower(i94mode.name) like 'not reported%', null, i94mode.name) as port_type
    from us_immigration.i94
        left join us_immigration.i94mode on i94.i94mode = i94mode.code
        left join us_immigration.i94port on i94.i94port = i94port.code
    where year || month || day = :day)
-- fact_immigration
SELECT
    i94.cicid as cic_id,
    i94.admnum,
    i94.datetime as arrival_date,
    date_add('day', i94.depdate, date '1960-01-01') as departure_date,
    port.port_code,
    port.port_type,
    airport.ident as airport_ident,
    split_part(airport.type, '_', 1) as airport_type,
    airport.name as airport,
    airport.municipality,
    dim_port.city,
    dim_port.state_code,
    i94.airline,
    i94.fltno as flight_number,
    i94visa.name as visa,
    birth.iso_country as birth_iso_country,
    residence.iso_country as residence_iso_country,
    i94.biryear as birth_year,
    case
      when i94.gender = 'F' then 'female'
      when i94.gender = 'M' then 'male'
    end as gender,
    i94.year,
    i94.month,
    i94.day
from us_immigration.i94
    join e_port as port on i94.cicid = port.cicid
    left join us_immigration.dim_country as birth
        on i94.i94cit = birth.i94country_code
    left join us_immigration.dim_country as residence
        on i94.i94res = residence.i94country_code
    left join us_immigration.i94visa
        on i94.i94visa = i94visa.code
    left join us_immigration.dim_port
        on port.port_code = dim_port.port_code
            and port.port_type = dim_port.port_type
    left join us_immigration.airport
        on dim_port.port_code = airport.iata_code
            and dim_port.port_type = 'air'
            and airport.iso_country = 'US'
where year || month || day = :day
;
        """)
        query = query.bindparams(day=self.day.strftime('%Y%m%d'))
        options = dict(compile_kwargs={"literal_binds": True})
        query = str(query.compile(**options))
        return query

    @property
    def insert(self):
        return f"""
            INSERT INTO {self.database}.{self.table}
        """

    @property
    def stmt(self):
        return self.insert + self.query


class UpsertFactImmigrationTable:

    def __init__(self, day: date, table, database, bucket, prefix):
        self.path = self._build_path(day, bucket, prefix, table)
        self.insert = InsertFactImmigrationTable(
            day=day, table=table, database=database
        )

    def _build_path(self, day, bucket, prefix, table):
        year = day.strftime('%Y')
        month = day.strftime('%m')
        day = day.strftime('%d')
        path = (
            bucket / prefix / table / f"year={year}" / f"month={month}" / f"day={day}"  # noqa: E501
        ).url
        return path

    def delete(self):
        wr.s3.delete_objects(self.path)

    def __call__(self):
        self.delete()
        self.insert()


class FactDemographicsTable(base.BaseQueryExecution):

    def __init__(self, table, bucket, prefix, database):
        super().__init__(database)
        self.table = table
        self.external_location = (bucket / prefix / table).url

    @property
    def ctas(self):
        return f"""
    CREATE TABLE IF NOT EXISTS {self.table}
    WITH (
      external_location='{self.external_location}',
      format='PARQUET',
      parquet_compression='SNAPPY',
      bucketed_by=ARRAY['city'],
      bucket_count=1
    ) AS
        """

    @property
    def query(self):
        return """

    select
       city,
       state_code,
       state,
       max(median_age) as median_age,
       cast(max(male_population) as integer) as male_population,
       cast(max(female_population) as integer) as female_population,
       max(total_population) as total_population,
       cast(max(number_of_veterans) as integer) as number_of_veterans,
       cast(max(foreign_born) as integer) as foreign_born,
       round(max(average_household_size), 2) as average_household_size,
       max(if(race='Asian', count, null)) as asia_population,
       max(if(race='Black or African-American', count, null)) as black_population,
       max(if(race='Hispanic or Latino', count, null)) as hispanic_population,
       max(if(race='American Indian and Alaska Native', count, null)) as native_population,
       max(if(race='White', count, null)) as white_population
    from us_immigration.demographics
    group by 1,2,3;

        """

    @property
    def stmt(self):
        return self.ctas + self.query

    @property
    def ddl(self):

        return f"""

CREATE EXTERNAL TABLE {self.table}(
  city string COMMENT '',
  state_code string COMMENT '',
  state string COMMENT '',
  median_age double COMMENT '',
  male_population int COMMENT '',
  female_population int COMMENT '',
  total_population bigint COMMENT '',
  number_of_veterans int COMMENT '',
  foreign_born int COMMENT '',
  average_household_size double COMMENT '',
  asia_population bigint COMMENT '',
  black_population bigint COMMENT '',
  hispanic_population bigint COMMENT '',
  native_population bigint COMMENT '',
  white_population bigint COMMENT '')
CLUSTERED BY (
  city)
INTO 1 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{self.external_location}'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'parquet.compression'='SNAPPY')

        """

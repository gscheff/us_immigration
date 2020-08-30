from furl import furl

from . import base


class DimCountry(base.Table):

    def __init__(self, table, database, bucket: furl, prefix):
        self.table = table
        self.database = database
        self.external_location = (bucket / prefix / table).url

    @property
    def ctas(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table}
        WITH (
          external_location='{self.external_location}',
          format='PARQUET',
          parquet_compression='SNAPPY',
          bucketed_by=ARRAY['iso_country'],
          bucket_count=1
        ) AS
        """

    @property
    def query(self):
        return """

    -- dim_port
    with cleaned_i94country as (
    select
       code,
       case
           when (lower(name) like 'invalid:%')
             or (lower(name) like 'no country code%')
             or (lower(name) like 'collapsed%')
           then null
           else name
       end as name
    from us_immigration.i94country)
    select
       i.code as i94country_code,
       country.code as iso_country,
       i.name as country,
       country.continent as continent
    from cleaned_i94country as i
    left join us_immigration.country on lower(i.name) = lower(country.name)
    order by iso_country
    ;

        """

    @property
    def ddl(self):

        return f"""

    CREATE EXTERNAL TABLE {self.table}(
      i94country_code bigint COMMENT '',
      iso_country string COMMENT '',
      country string COMMENT '',
      continent string COMMENT '')
    CLUSTERED BY (
      iso_country)
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
    def insert(self):
        return f"""
            INSERT INTO {self.table}
        """

    @property
    def drop(self):
        return f"""
            DROP TABLE {self.table}
        """


class DimPort(base.Table):

    def __init__(self, table, database, bucket: furl, prefix):
        self.table = table
        self.database = database
        self.external_location = (bucket / prefix / table).url

    @property
    def ctas(self):

        return f"""
    CREATE TABLE IF NOT EXISTS {self.table}
    WITH (
      external_location='{self.external_location}',
      format='PARQUET',
      parquet_compression='SNAPPY',
      bucketed_by=ARRAY['port_code'],
      bucket_count=1
    ) AS
    """

    @property
    def query(self):
        return """

    -- dim_port
    with etl_port
    as (
    select
      if(i94port.code = 'XXX', null, i94port.code) as port_code,
      if(lower(i94mode.name) = 'not reported', null, i94mode.name)
          as port_type,
      if((lower(i94port.name) like 'not reported%')
          or (lower(i94port.name) like 'no port%'),
         null, trim(split_part(i94port.name, ', ', 1))) as city,
      case
          when length(split_part(i94port.name, ', ', 2)) = 2
            then upper(split_part(i94port.name, ', ', 2))
      end as state_code,
      case
          when length(split_part(i94port.name, ', ', 2)) > 2
            then split_part(i94port.name, ', ', 2)
      end as comment
    from us_immigration.i94
      left join us_immigration.i94mode on i94.i94mode = i94mode.code
      left join us_immigration.i94port on i94.i94port = i94port.code
    group by 1, 2, 3, 4, 5
    order by 1)
    --
    select
      *
    from etl_port
    where port_type is not null
      and lower(port_type) != 'not reported'
    ;

    """

    @property
    def ddl(self):

        return f"""

    CREATE EXTERNAL TABLE {self.table}(
      port_code string COMMENT '',
      port_type string COMMENT '',
      city string COMMENT '',
      state_code string COMMENT '',
      comment string COMMENT '')
    CLUSTERED BY (
      port_code)
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
    def insert(self):
        return f"""
            INSERT INTO {self.table}
        """

    @property
    def drop(self):
        return f"""
            DROP TABLE {self.table}
        """


class DimState(base.Table):

    def __init__(self, table, database, bucket: furl, prefix):
        self.table = table
        self.database = database
        self.external_location = (bucket / prefix / table).url

    @property
    def ctas(self):
        return f"""
    CREATE TABLE IF NOT EXISTS {self.table}
    WITH (
      external_location='{self.external_location}',
      format='PARQUET',
      parquet_compression='SNAPPY',
      bucketed_by=ARRAY['state_code'],
      bucket_count=1
    ) AS
    """

    @property
    def query(self):
        return """

    select local_code as state_code,
       name as state,
       'US' as iso_country
    from us_immigration.region
    where iso_country = 'US'
      and length(local_code) = 2;

    """

    @property
    def ddl(self):

        return f"""

    CREATE EXTERNAL TABLE {self.table}(
      state_code string COMMENT '',
      state string COMMENT '',
      iso_country varchar(2) COMMENT '')
    CLUSTERED BY (
      state_code)
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
    def insert(self):
        return f"""
            INSERT INTO {self.table}
        """

    @property
    def drop(self):
        return f"""
            DROP TABLE {self.table}
        """

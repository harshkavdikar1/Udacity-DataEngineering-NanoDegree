class SqlQueries:
    table_name = ['staging_immigration','immigration', 'i94cit_i94res', 'i94mode','i94addr','i94visa','i94port']


    drop_table = """
    DROP TABLE IF EXISTS {};
    """
    immigrant_table_create = """CREATE TABLE IF NOT EXISTS public.immigration (
        cicid FLOAT PRIMARY KEY,
        i94yr FLOAT,
        i94mon FLOAT,
        i94cit FLOAT,
        i94res FLOAT,
        i94port VARCHAR,
        arrdate FLOAT,
        i94mode FLOAT,
        i94addr VARCHAR,
        depdate FLOAT,
        i94bir FLOAT,
        i94visa FLOAT,
        count FLOAT,
        dtadfile VARCHAR,
        visapost VARCHAR,
        occup VARCHAR,
        entdepa VARCHAR,
        entdepd VARCHAR,
        entdepu VARCHAR,
        matflag VARCHAR,
        biryear FLOAT,
        dtaddto VARCHAR,
        gender VARCHAR,
        insnum VARCHAR,
        airline VARCHAR,
        admnum FLOAT,
        fltno VARCHAR,
        visatype VARCHAR
        );
    """

    us_cities_demographics_table_create = """
    CREATE TABLE IF NOT EXISTS public.us_cities_demographics (
    city                       VARCHAR,
    state                      VARCHAR, 
    median_age                 FLOAT, 
    male_population            FLOAT,
    female_Population          FLOAT,
    total_Population           FLOAT,
    number_veterans            FLOAT,
    foreign_born               FLOAT,
    average_household_size     FLOAT,
    state_code                 VARCHAR,
    race                       VARCHAR,
    count                      INT
    );
    """

    us_cities_table_create = """
    CREATE TABLE IF NOT EXISTS public.us_cities (
    city                       VARCHAR,
    state                      VARCHAR, 
    median_age                 FLOAT, 
    male_population            FLOAT,
    female_Population          FLOAT,
    total_Population           FLOAT,
    number_veterans            FLOAT,
    foreign_born               FLOAT,
    average_household_size     FLOAT,
    state_code                 VARCHAR
    );
    """
    
    airport_table_create = """
    CREATE TABLE IF NOT EXISTS public.airport (
    ident            VARCHAR,
    type             VARCHAR,
    name             VARCHAR,
    elevation_ft     FLOAT,
    continent        VARCHAR,
    iso_country      VARCHAR,
    iso_region       VARCHAR,
    municipality     VARCHAR,
    gps_code         VARCHAR,
    iata_code        VARCHAR,
    local_code       VARCHAR,
    coordinates      VARCHAR
    )
    """
    
    # Clean airports dataset filtering only US airports with iata code
    # discarting anything else that is not an airport.
    clean_airport_table = """
    DELETE FROM public.airport
    WHERE iata_code ='' OR 
          iso_country != 'US' OR 
          type not in('large_airport', 'medium_airport', 'small_airport')
    """

    ## I94CIT & I94RES - This format shows all the valid and invalid codes for processing 
    i94cit_i94res_table_create = """
    CREATE TABLE IF NOT EXISTS public.i94cit_i94res (
        code SMALLINT PRIMARY KEY,
        country VARCHAR
        );
    """
    
 

    ## /* I94PORT - This format shows all the valid and invalid codes for processing */
    i94port_table_create = """
    CREATE TABLE IF NOT EXISTS public.i94port (
            code VARCHAR PRIMARY KEY,
            port_of_entry VARCHAR,
            city VARCHAR,
            state_or_country VARCHAR
        );
    """

    ## I94MODE - There are missing values as well as not reported (9)
    i94mode_table_create = """
    CREATE TABLE IF NOT EXISTS public.i94mode (
        code SMALLINT PRIMARY KEY,
        transportation VARCHAR
        );
    """

    ## I94ADDR - There is lots of invalid codes in this variable and the list below 
    ## shows what we have found to be valid, everything else goes into 'other'
    i94addr_table_create = """
    CREATE TABLE IF NOT EXISTS public.i94addr (
        code VARCHAR PRIMARY KEY,
        state VARCHAR
        );
    """
    ## I94VISA - Visa codes collapsed into three categories:
    ## 1 = Business
    ## 2 = Pleasure
    ## 3 = Student
    i94visa_table_create = """
    CREATE TABLE IF NOT EXISTS public.i94visa (
        code SMALLINT PRIMARY KEY,
        reason_for_travel VARCHAR
        );
    """
    
    us_state_race_table_create = """
    CREATE TABLE IF NOT EXISTS public.us_state_race (
    state_code VARCHAR,
    state VARCHAR,
    race VARCHAR,
    race_ratio FLOAT
    )
    """

    us_state_race_table_insert ="""
    SELECT a.state_code,
           a.state,
           race,
           cast (AVG(cast (a.count AS float)/b.total) AS decimal(2, 2))
    FROM us_cities_demographics a
    JOIN (
    SELECT state_code,city,SUM(count) AS total
    FROM us_cities_demographics
    GROUP BY state_code,city
    ORDER BY state_code,city) b
    ON (a.state_code = b.state_code AND
      a.city = b.city)
    GROUP BY a.state_code,a.state,a.race
    ORDER BY a.state_code
    """

    us_cities_table_insert = """
    SELECT DISTINCT city,
                    state, 
                    median_age, 
                    male_population,
                    female_Population,
                    total_Population,
                    number_veterans,
                    foreign_born,
                    average_household_size,     
                    state_code    
    FROM public.us_cities_demographics                           
    """ 
    



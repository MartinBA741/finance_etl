create_staging_hist = """
    CREATE TABLE public.staging_hist (
        Ticker varchar(256) NOT NULL,
        Date timestamp,
        Open numeric(18,0),
        High numeric(18,0),
        Low numeric(18,0),
        Close numeric(18,0),
        AdjClose numeric(18,0),
        Volume numeric(18,0),
        PRIMARY KEY (Ticker, Date)
    );
    """

create_FactHist = """
    CREATE TABLE public.FactHist (
    	Ticker varchar(256) NOT NULL,
        Date timestamp,
        AdjClose numeric(18,0),
        return numeric(18,0),
        Company varchar(256),
        CEO varchar(256),
        Founded timestamp,
        PRIMARY KEY (Ticker, Date)
    );
    """

create_DimCorp = """
    CREATE TABLE public.DimCorp (
    	Ticker varchar(256) NOT NULL,
        Company varchar(256),
        CEO varchar(256),
        Founded timestamp,
        PRIMARY KEY (Ticker, Date)
    );
    """


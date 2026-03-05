import pandas as pd
from config_903 import DateCols, EthnicSubcatgories
from dateutil.relativedelta import relativedelta

def format_dates(column): 
    ''' 
    replaces empty strings and spaces with pandas not a time values and 
    replaces any NaNs with NaTs
    warn user if date format is wrong expectes %d/%m/%Y and says what column is wrong
    '''
    column.replace(r"^\s*$", pd.NaT, regex=True) # can use regex to find the empty strings ^\s*$ is the regex for blank space
    column = column.fillna(pd.NaT)
    try: 
        column  = pd.to_datetime(column, format="%d/%m/%Y")
        return column
    except: 
        raise ValueError(f"Unknown date format in {column.name}, expected dd/mm/YYYY")
    
    
def calculate_age_buckets(age):
    ''' Used to make age buckets matching published data'''
    if age < 1:
        return "a) Under 1 year"
    elif age < 5:
        return "b) 1 to 4 years"
    elif age < 10:
        return "c) 5 to 9 years"
    elif age < 16:
        return "d) 10 to 16 years"
    elif age >= 16:
        return "e) 16 years and over"
    else:
        return "f) Age error"
    


def clean_903_table(df: pd.DataFrame, collection_end: pd.Timestamp) -> pd.DataFrame: 
    ''' 
    Takes tables from the 903 as dataframes and outputs cleaned tables. 
    '''
    clean_df = df.copy() # make a copy that is worked on so not overwriitng orrginal data

    # remove index
    if "index" in df.columns:
        clean_df.drop("index", axis=1, inplace=True) # when axis = 0 it goes by row. when axis = 0 it drops rows

    # convert date columns 
    for column in clean_df.columns:
        if column in DateCols.cols.value:
            clean_df[f"{column}_dt"] = format_dates(clean_df[column]) # makes a new column with the suffix _dt
        
            
    #  Make ethnic main group column. rather than maping with a dictionary we are using enums as it it more efficient

    if "Ethnic" in clean_df.columns:

        clean_df['ETHNICITY'] = clean_df['ETHNIC'].apply(   #using a lambda function
            lambda ethnicity: EthnicSubcatgories[ethnicity].value
        ) 
    
    #  make age column
    if "DOB_dt" in clean_df.columns:
        clean_df['AGE'] = clean_df['DOB_dt'].apply(
            lambda dob: relativedelta(dt1=collection_end, dt2=dob).normalized().years  #dt1 is end date of collection. normalized takes it at midnight
        )
    #  add age buckets column
        clean_df['AGE_BUCKETS'] = clean_df['AGE'].apply(calculate_age_buckets)

    return clean_df
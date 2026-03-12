import pandas as pd
from config_903 import DateCols, EthnicSubcategories
from dateutil.relativedelta import relativedelta
import numpy as np

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

    if "ETHNIC" in clean_df.columns:

        clean_df['ETHNICITY'] = clean_df['ETHNIC'].apply(   #using a lambda function
            lambda x: EthnicSubcategories[x].value
        ) 

    #  make age column
    if "DOB_dt" in clean_df.columns:
        clean_df['AGE'] = clean_df['DOB_dt'].apply(
            lambda dob: relativedelta(dt1=collection_end, dt2=dob).normalized().years  #dt1 is end date of collection. normalized takes it at midnight
        )
    #  add age buckets column
        clean_df['AGE_BUCKETS'] = clean_df['AGE'].apply(calculate_age_buckets)

    return clean_df


# def group_calculation(df, column, measure_name):
#     '''
#     A function to group a df by input column with the outputs with count,
#     and percentage to a dataframe with renamed columns.
#     '''
#     grouped = df.groupby(column).size() # before function specified table as dfs['header']
#     #grouped = grouped.to_frame('Count') # makes it a multi idnex chart / makes count  aheader name 
#     grouped = grouped.to_frame(f'{measure_name} - Count').reset_index() # then makes it like an actual table, 
#     grouped = grouped.rename(columns = {column: 'value'}) # renaming column to make it lower case
#     #could have used things like .capitalise() or .lower() or .upper()

#     grouped[f'{measure_name} - Percentage'] = (grouped[f'{measure_name} - Count'] / 
#                                                     grouped[f'{measure_name} - Count'].sum()) * 100 

    
#     #print(grouped)
#     #print(grouped[f'{measure_name} - Percentage'].sum())
#     return grouped

def group_calculation(df, column, measure_name):
    '''
    A function to group as df by input column, outputs with count
    and percentage to a dataframe with renamed columns.
    '''
    grouped = df.groupby(column).size()
    grouped = grouped.to_frame(f'{measure_name} - Count').reset_index()
    grouped = grouped.rename(columns={column:'Value'})

    grouped[f'{measure_name} - Percentage'] = (grouped[f'{measure_name} - Count'] / 
                                                    grouped[f'{measure_name} - Count'].sum()) * 100
    
    return grouped

def time_difference(start_col, end_col, business_days=False):
    ''' 
    Takes 2 date columns and returns difference in time. 
    can also be used to find differnece in buisness days. if say buisness_days=true
    '''

    if business_days: 
        #np.busday count can only use datetime64[D] type data
        #so must convert the objects
        time_diff = np.busday_count(
            start_col.values.astype("datetime64[D]"),
            end_col.values.astype("datetime64[D]")
        )
    else:
        time_diff = end_col - start_col
        time_diff = time_diff / pd.Timedelta(days=1)
    return time_diff.astype('int')

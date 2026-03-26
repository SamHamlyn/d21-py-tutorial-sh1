import pytest
import pandas as pd # as eventually wil run tests that use it 

def sum_xy(x, y): 
    """Adds x and y together"""
    return x + y

def test_sum_xy(): 
    """starts with test_ so pytest finds it"""
    result = sum_xy(x=1, y=1)
    assert result == 2

#Task: 
# write a function that multiplies two number
#wreite a test to check that it works

def prod_xy(x, y):
    "Multiples two inputs together"
    return x*y

def test_prod_xy():
    """A test for the prod_xy function does 3*2 =6"""
    result = prod_xy(x=3, y=2)
    assert result == 6

def sumprod(x,y):
    """runs both sumxy and prodxy"""
    return sum_xy(x,y), prod_xy(x,y)
    # return prod_xy(x,y)

# def test_sumprod():
#     sum, prod = sumprod(4,9)
#     assert sum == 13
#     assert prod == 36

def test_sumprod():
    sum, prod = sumprod(4, 9)

    assert sum == 13
    assert prod == 36

def df_slicer(df, age): 
    age_or_over = df[df['Age'] >= age]
    return age_or_over

def test_df_age_slicer(): 
    """has a test dataframe that we use to test with. Uses age 5 as test"""
    test_df = pd.DataFrame(
        [
            {"ChildId": "child1", "Age": 6},
            {"ChildId": "child3", "Age": 10},
            {"ChildId": "child2", "Age": 4},
            {"ChildId": "child4", "Age": 1},
        ]
    )
    sliced_df = df_slicer(test_df, 5)

    expected_df = pd.DataFrame(
        [
            {"ChildId": "child1", "Age": 6},
            {"ChildId": "child3", "Age": 10},
         ]   
    )
    # to compare dataframes it is easier to use pandas inbuilt testing tools
    
    pd.testing.assert_frame_equal(sliced_df, expected_df, check_names=None) 
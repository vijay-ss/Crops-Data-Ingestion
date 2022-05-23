import pandas as pd

def update_region(row):
    
    if row['State_Code'] == 2:
        row['EZT_Region_Key'] = 60
        row['Region_Name'] = 'USDA - West'
    if row['State_Code'] == 33:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 35:
        row['EZT_Region_Key'] = 60
        row['Region_Name'] = 'USDA - West'
    if row['State_Code'] == 44:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 50:
        row['EZT_Region_Key'] = 57
        row['Region_Name'] = 'USDA - Northeast'
    if row['State_Code'] == 60:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 66:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 69:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 72:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other'
    if row['State_Code'] == 78:
        row['EZT_Region_Key'] = 99
        row['Region_Name'] = 'Other' 
        
    return row


def test_method1(update_region):
    x = 17
    assert update_region(0) == x


def test_method2(update_region):
    y = 'test_string'
    assert update_region('test') == y

test_method2(update_region)

d = {'State_Code': [44, 12], 'EZT_Region_Key': [66, 67], 'Region_Name': ['testa', 'testb']}
df = pd.DataFrame(data=d)

df.apply(update_region, axis=1)


def test_method3(update_region):
    y = 'test_string'
    assert update_region(df) == 12

test_method3(update_region)

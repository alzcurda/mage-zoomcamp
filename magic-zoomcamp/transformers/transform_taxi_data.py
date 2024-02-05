from re import sub

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def snake_case(s):
    # Replace hyphens with spaces, then apply regular expression substitutions for title case conversion
    # and add an underscore between words, finally convert the result to lowercase
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',
        sub('([A-Z]+)', r' \1',
        s.replace('-', ' '))).split()).lower()

@transformer
def transform(data, *args, **kwargs):
    print(f"Preprocessing: Initial rows: {data.shape[0]}")
    print(f"Preprocessing: rows with 0 or null passengers: {data['passenger_count'].isin([0]).sum()+data['passenger_count'].isnull().sum()}")

    filter_zero_passengers = data['passenger_count']>0

    print(f"Preprocessing: rows with zero passengers filtererd: {filter_zero_passengers.sum()}")

    filter_zero_trip_distance = data['trip_distance']>0

    print(f"Preprocessing: rows with trip distance zero filtererd: {filter_zero_trip_distance.sum()}")

    taxi_df = data.loc[filter_zero_passengers & filter_zero_trip_distance]

    """
    Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    """
    taxi_df = taxi_df.assign(lpep_pickup_date=data['lpep_pickup_datetime'].dt.date).rename(columns=lambda x: snake_case(x))
    
    print(f"Preprocessing: Unique vendor_id values: {taxi_df['vendor_id'].unique()}")

    print(f"Columns modified: {data.columns.difference(taxi_df.columns)}")

    return taxi_df


@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are trips with 0 passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are trips with 0 trip distance'
    assert 'vendor_id' in output.columns, 'Column vendor_id not exists'

    
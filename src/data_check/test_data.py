import pandas as pd
import numpy as np
import scipy.stats

def test_column_names(data):
    """
    Test that the DataFrame 'data' has exactly the expected columns, in the correct order.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be tested.

    Raises
    ------
    AssertionError
        If the DataFrame's columns do not exactly match the expected columns.
    """
    # Define the expected column names and order.
    expected_columns = [
        "id",
        "name",
        "host_id",
        "host_name",
        "neighbourhood_group",
        "neighbourhood",
        "latitude",
        "longitude",
        "room_type",
        "price",
        "minimum_nights",
        "number_of_reviews",
        "last_review",
        "reviews_per_month",
        "calculated_host_listings_count",
        "availability_365",
    ]

    # Get the actual column names from the DataFrame.
    these_columns = data.columns.values

    # Assert that the expected and actual column names match exactly in both content and order.
    assert list(expected_columns) == list(these_columns)


def test_neighborhood_names(data):
    """
    Test that the 'neighbourhood_group' column in the DataFrame contains the known neighborhood names.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be tested.

    Raises
    ------
    AssertionError
        If the unique values in 'neighbourhood_group' do not match the known names.
    """
    # Define the known neighborhood group names.
    known_names = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]

    # Get the unique neighborhood group names from the DataFrame.
    neigh = set(data['neighbourhood_group'].unique())

    # Compare the sets (order doesn't matter) and assert they are equal.
    assert set(known_names) == set(neigh)


def test_proper_boundaries(data: pd.DataFrame):
    """
    Test that all properties in the DataFrame 'data' have longitude and latitude within 
    the expected boundaries for NYC (and its surrounding area).

    Expected boundaries:
        - Longitude between -74.25 and -73.50
        - Latitude between 40.5 and 41.2

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be tested.

    Raises
    ------
    AssertionError
        If any property's longitude or latitude falls outside the specified boundaries.
    """
    # Create a boolean mask indicating whether each row's longitude and latitude are within bounds.
    idx = data['longitude'].between(-74.25, -73.50) & data['latitude'].between(40.5, 41.2)

    # Assert that every row in the DataFrame meets the boundary criteria.
    # np.sum(~idx) counts how many rows do not meet the criteria.
    assert np.sum(~idx) == 0


def test_similar_neigh_distrib(data: pd.DataFrame, ref_data: pd.DataFrame, kl_threshold: float):
    """
    Test that the distribution of the 'neighbourhood_group' in the new data is similar 
    to that in the reference data using Kullback-Leibler divergence.

    Parameters
    ----------
    data : pd.DataFrame
        The new dataset to test.
    ref_data : pd.DataFrame
        The reference dataset with known distribution.
    kl_threshold : float
        The maximum acceptable Kullback-Leibler divergence.

    Raises
    ------
    AssertionError
        If the KL divergence between the new data distribution and the reference distribution
        is greater than or equal to the threshold.
    """
    # Compute the value counts for each neighbourhood group in both datasets and sort by index.
    dist1 = data['neighbourhood_group'].value_counts().sort_index()
    dist2 = ref_data['neighbourhood_group'].value_counts().sort_index()

    # Compute the Kullback-Leibler divergence (entropy) between the two distributions.
    divergence = scipy.stats.entropy(dist1, dist2, base=2)
    
    # Assert that the divergence is below the specified threshold.
    assert divergence < kl_threshold


def test_row_count(data):
    """
    Test that the DataFrame 'data' has a reasonable number of rows.
    
    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be tested.
    
    Raises
    ------
    AssertionError
        If the number of rows is not between 15,000 and 1,000,000.
    """
    # Check that the number of rows is within the expected range.
    assert 15_000 <= data.shape[0] <= 1_000_000


def test_price_range(data, min_price, max_price):
    """
    Test that all values in the 'price' column of the DataFrame 'data' are within the specified range.
    
    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be tested.
    min_price : int or float
        The minimum acceptable price.
    max_price : int or float
        The maximum acceptable price.
    
    Raises
    ------
    AssertionError
        If any price is outside the specified range.
    """
    # Check that every price value is between min_price and max_price.
    assert data['price'].between(min_price, max_price).all()
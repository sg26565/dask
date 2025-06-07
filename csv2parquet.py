"""
This module provides a function to convert large CSV files to Parquet files using Dask,
optimizing for memory efficiency.
"""

import dask.dataframe as dd

def convert_csv_to_parquet(in_path: str, out_path: str, dtypes: dict[str, str], bs: str = "100MB") -> None:
    """
    Convert a large CSV file to a Parquet file using Dask, handling large files efficiently.

    Parameters:
    input_csv_path (str): Path to the input CSV file.
    output_parquet_path (str): Path to the output Parquet file.
    dtypes (dict): Dictionary specifying the data types of the columns.
    blocksize (str): Size of the chunks to be processed by Dask (e.g. "100MB")
    """
    # Read the CSV file in streaming mode using Dask with specified dtypes and block size
    dask_df = dd.read_csv(in_path, dtype=dtypes, blocksize=bs)

    # Perform any data transformations here (e.g., filtering, renaming, etc.)
    # Example: dask_df = dask_df[dask_df["some_column"] > 10]

    # Write the data to a Parquet file efficiently using pyarrow
    dask_df.to_parquet(out_path, engine='pyarrow')


if __name__ == "__main__":
    INPUT_CSV_PATH = '5000000 HRA Records.csv'
    OUTPUT_PARQUET_PATH = '5000000 HRA Records'

    # Define the column data types
    DTYPES = {
        'Age': 'int64',
        'Attrition': 'object',
        'BusinessTravel': 'object',
        'DailyRate': 'int64',
        'Department': 'object',
        'DistanceFromHome': 'int64',
        'Education': 'int64',
        'EducationField': 'object',
        'EmployeeCount': 'int64',
        'EmployeeNumber': 'int64',
        'EnvironmentSatisfaction': 'int64',
        'Gender': 'object',
        'HourlyRate': 'int64',
        'JobInvolvement': 'int64',
        'JobLevel': 'int64',
        'JobRole': 'object',
        'JobSatisfaction': 'int64',
        'MaritalStatus': 'object',
        'MonthlyIncome': 'int64',
        'MonthlyRate': 'int64',
        'NumCompaniesWorked': 'int64',
        'Over18': 'object',
        'OverTime': 'object',
        'PercentSalaryHike': 'int64',
        'PerformanceRating': 'int64',
        'RelationshipSatisfaction': 'int64',
        'StandardHours': 'int64',
        'StockOptionLevel': 'int64',
        'TotalWorkingYears': 'int64',
        'TrainingTimesLastYear': 'int64',
        'WorkLifeBalance': 'int64',
        'YearsAtCompany': 'int64',
        'YearsInCurrentRole': 'int64',
        'YearsSinceLastPromotion': 'int64',
        'YearsWithCurrManager': 'int64'
    }

    convert_csv_to_parquet(INPUT_CSV_PATH, OUTPUT_PARQUET_PATH, DTYPES, bs = "100MB")

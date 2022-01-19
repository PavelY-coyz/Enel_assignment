import numpy as np
import pytest
import pandas as pd
from datetime import datetime
from modules import DataStream


@pytest.fixture
def global_vars():
	import config
	return {"path": config.csv_path_test,
	        "file_pattern": config.stream_id_pattern,
	        "valid_column_names": config.valid_column_names,
	        "logger": config.logger}


def test_get_stream_id(global_vars):
	# valid filenames are already checked for in csv_local_test
	file_name = "2.csv"
	pattern = global_vars["file_pattern"]

	stream_id = DataStream.get_stream_id(pattern=pattern, file=file_name)
	assert type(stream_id) == int
	assert stream_id == 2


def test_get_data_stream(global_vars):
	path = f"{global_vars['path']}get_data_stream_test/2.csv"
	file_name = "2.csv"
	pattern = global_vars["file_pattern"]

	stream_id = DataStream.get_stream_id(pattern=pattern, file=file_name)

	res = DataStream.get_data_stream(stream_id=stream_id,
	                                 file_path=path,
	                                 valid_column_names=global_vars["valid_column_names"])

	"""Make sure it is of type Ddtastream"""
	assert type(res) == DataStream.DataStream

	"""Check the data frame create with the Datastream to make sure it is what we expect"""
	df = res.get_df().astype({"dttm_utc": "datetime64[ns]"})

	columns = ["stream_id", "timestamp", "dttm_utc", "value", "estimated", "anomaly"]
	rows = np.array([[stream_id, 1325376600, pd.to_datetime("2012-01-01 00:10:00"), 7.2386, 0, np.nan],
	                 [stream_id, 1325376900, pd.to_datetime("2012-01-01 00:15:00"), 6.6226, 0, np.nan],
	                 [stream_id, 1325377200, pd.to_datetime("2012-01-01 00:20:00"), 6.9306, 0, np.nan],
	                 [stream_id, 1325377500, pd.to_datetime("2012-01-01 00:25:00"), 7.0846, 0, np.nan]])

	expected_df = pd.DataFrame(rows, columns=columns).reset_index().drop("index", axis=1).astype({
		"stream_id": "int64", "timestamp": "int64", "dttm_utc": "datetime64[ns]", "value": "float64",
		"estimated": "int64", "anomaly": "float64"
	})

	"""Make sure the dataframes are equal"""
	assert df.equals(expected_df) == True

	# change the last value of the dataframe (from 7.0846 to 7.0847) - just to make sure it isnt equal
	different_rows = np.array([
		[stream_id, 1325376600, pd.to_datetime("2012-01-01 00:10:00"), 7.2386, 0, np.nan],
		[stream_id, 1325376900, pd.to_datetime("2012-01-01 00:15:00"), 6.6226, 0, np.nan],
		[stream_id, 1325377200, pd.to_datetime("2012-01-01 00:20:00"), 6.9306, 0, np.nan],
		[stream_id, 1325377500, pd.to_datetime("2012-01-01 00:25:00"), 7.0847, 0, np.nan]])

	different_df = pd.DataFrame(different_rows, columns=columns).reset_index().drop("index", axis=1).astype({
		"stream_id": "int64", "timestamp": "int64", "dttm_utc": "datetime64[ns]", "value": "float64",
		"estimated": "int64", "anomaly": "float64"
	})

	assert df.equals(different_df) == False


def test_calculate_stream_level_data(global_vars):
	"""Test a valid file without any 0's or NaN's"""
	path = f"{global_vars['path']}calculate_stream_level_data_test/2.csv"
	file_name = "2.csv"
	pattern = global_vars["file_pattern"]

	stream_id = DataStream.get_stream_id(pattern=pattern, file=file_name)

	ds = DataStream.get_data_stream(stream_id=stream_id,
	                                file_path=path,
	                                valid_column_names=global_vars["valid_column_names"])

	stream_df = pd.DataFrame()
	stream_df = DataStream.calculate_stream_level_data(ds=ds,
	                                                   stream_df=stream_df)

	columns = ["stream_id", "status", "message", "rank", "% of 0 and NaN", "% of 0", "% of NaN", "count of 0 and NaN",
	           "count of 0's", "count of NaN", "ignore"]
	rows = np.array([[stream_id, "Processed", None, np.nan, 0, 0, 0, 0, 0, 0, False]])
	type_list = {"stream_id": "int64",
	             "rank": "float64",
	             "% of 0 and NaN": "float64",
	             "% of 0": "float64",
	             "% of NaN": "float64",
	             "count of 0 and NaN": "int64",
	             "count of 0's": "int64",
	             "count of NaN": "int64",
	             "ignore": "bool"}

	expected_df = pd.DataFrame(rows, columns=columns).reset_index().drop("index", axis=1).astype(type_list)

	assert stream_df.equals(expected_df) == True


def test_calculate_stream_level_data_with_zeros_and_nans(global_vars):
	"""Test a valid file with a mix of zero's and NaN's"""

	# TODO move this outside of this function - as it gets used multiple time
	def get_ds_obj(s_id):
		path = f"{global_vars['path']}calculate_stream_level_data_test/{s_id}.csv"
		file_name = f"{s_id}.csv"
		pattern = global_vars["file_pattern"]

		stream_id = DataStream.get_stream_id(pattern=pattern, file=file_name)

		ds = DataStream.get_data_stream(stream_id=stream_id,
		                                file_path=path,
		                                valid_column_names=global_vars["valid_column_names"])

		stream_df = pd.DataFrame()
		stream_df = DataStream.calculate_stream_level_data(ds=ds,
		                                                   stream_df=stream_df)

		return stream_df, ds

	tests = [
		{
			"id": 3,
			"desc": 'Test file "3.csv" - contains two zero values, and two non-zero values \
			Expected outcome - ignore flag should be set to false (since not all values are zero/nan/1',
			"expected_result": np.array([[3, "Processed", None, np.nan, 0.5000, 0.5000, 0, 2, 2, 0, False]])
		},
		{
			"id": 4,
			"desc": 'Test file "4.csv" - contains two zero values, one nan, and one non-zero values \
			Expected outcome - ignore flag should be set to false (still 1 valid value)',
			"expected_result": np.array([[4, "Processed", None, np.nan, 0.7500, 0.5000, 0.2500, 3, 2, 1, False]])
		},
		{
			"id": 5,
			"desc": 'Test file "5.csv" - contains two zero values and two nan \
			 Expected outcome - ignore flag should be set to true',
			"expected_result": np.array([[5, "Processed", None, np.nan, 1.0000, 0.5000, 0.5000, 4, 2, 2, True]])
		},
		{
			"id": 6,
			"desc": 'Test file "6.csv" - contains two zero values and two one\'s \
			Expected outcome - ignore flag should be set to true',
			"expected_result": np.array([[6, "Processed", None, np.nan, 0.5000, 0.5000, 0, 2, 2, 0, True]])
		},
		{
			"id": 7,
			"desc": 'Test file "7.csv" - contains headers but no data; ignore should be set to True',
			"expected_result": np.array([[7, "Warning", "Empty file - Structure is correct but has no data",
			                              np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, True]])
		},
		{
			"id": 8,
			"desc": 'Test file "8.csv" - contains either - invalid headers or just an empty file',
			"expected_result": np.array([[8, "Error", "doesnt matter",
			                              np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, True]])
		}
	]

	columns = ["stream_id", "status", "message", "rank", "% of 0 and NaN", "% of 0", "% of NaN", "count of 0 and NaN",
	           "count of 0's", "count of NaN", "ignore"]
	type_list = {"stream_id": "int64",
	             "rank": "float64",
	             "% of 0 and NaN": "float64",
	             "% of 0": "float64",
	             "% of NaN": "float64",
	             "count of 0 and NaN": "int64",
	             "count of 0's": "int64",
	             "count of NaN": "int64",
	             "ignore": "bool"}

	# specifically for case 7 and 8; because nan's are auto converted to float64 by Pandas
	type_list2 = {"stream_id": "int64",
	              "rank": "float64",
	              "% of 0 and NaN": "float64",
	              "% of 0": "float64",
	              "% of NaN": "float64",
	              "count of 0 and NaN": "float64",
	              "count of 0's": "float64",
	              "count of NaN": "float64",
	              "ignore": "bool"}

	for test in tests:
		stream_id = test["id"]
		print(f"Starting test case: {test['id']}")
		stream_df, ds = get_ds_obj(stream_id)

		try:
			expected_df = pd.DataFrame(test["expected_result"],
			                           columns=columns).reset_index().drop("index", axis=1).astype(type_list)
		except ValueError as e:
			expected_df = pd.DataFrame(test["expected_result"],
			                           columns=columns).reset_index().drop("index", axis=1).astype(type_list2)

		if stream_id == 8:
			stream_df = stream_df.drop("message", axis=1)
			expected_df = expected_df.drop("message", axis=1)

		print(test["desc"])
		# print(stream_df.head())
		# print(expected_df.head())
		#
		# print(stream_df.info())
		# print(expected_df.info())

		print("values")
		print(str(stream_df.values))
		print(str(expected_df.values))

		# print(ds.__df__.values)
		assert stream_df.equals(expected_df) == True


def test_calculate_interval_level_data(global_vars):
	"""Test for valid calculations and groupings"""
	# TODO move this outside of this function - as it gets used multiple time
	def get_ds_obj(s_id):
		path = f"{global_vars['path']}calculate_interval_level_data_test/{s_id}.csv"
		file_name = f"{s_id}.csv"
		pattern = global_vars["file_pattern"]

		stream_id = DataStream.get_stream_id(pattern=pattern, file=file_name)

		ds = DataStream.get_data_stream(stream_id=stream_id,
		                                file_path=path,
		                                valid_column_names=global_vars["valid_column_names"])

		return ds

	stream_id = 1
	ds = get_ds_obj(stream_id)

	# daily/hourly calculations on a dataset with nan values
	tests = [
		# daily interval cases
		{
			"id": 1,
			"desc": "This is a test of the max function for the daily grouping",
			"config": {
				"interval": {
					"definition": ds.generate_daily_grouping,
					"calcs": [
						{"column_name": "daily_max", "calc_type": "max"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([52.1147, 50.9794])
		},
		{
			"id": 2,
			"desc": "This is a test of the min function for the daily grouping",
			"config": {
				"interval": {
					"definition": ds.generate_daily_grouping,
					"calcs": [
						{"column_name": "daily_min", "calc_type": "min"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([42.3951, 26.9435])
		},
		{
			"id": 3,
			"desc": "This is a test of the median function for the daily grouping",
			"config": {
				"interval": {
					"definition": ds.generate_daily_grouping,
					"calcs": [
						{"column_name": "daily_median", "calc_type": "median"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([49.8164, 41.2875])
		},
		{
			"id": 4,
			"desc": "This is a test of the mean function for the daily grouping",
			"config": {
				"interval": {
					"definition": ds.generate_daily_grouping,
					"calcs": [
						{"column_name": "daily_mean", "calc_type": "mean"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([47.65092, 39.7368])
		},

		# Hourly Interval test cases
		{
			"id": 5,
			"desc": "This is a test of the max function for the hourly grouping",
			"config": {
				"interval": {
					"definition": ds.generate_hourly_grouping,
					"calcs": [
						{"column_name": "hourly_max", "calc_type": "max"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([52.1147, 42.9767, 50.9794])
		},
		{
			"id": 6,
			"desc": "This is a test of the min function for the hourly grouping",
			"config": {
				"interval": {
					"definition": ds.generate_hourly_grouping,
					"calcs": [
						{"column_name": "hourly_min", "calc_type": "min"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([49.8164, 42.3951, 26.9435])
		},
		{
			"id": 7,
			"desc": "This is a test of the median function for the hourly grouping",
			"config": {
				"interval": {
					"definition": ds.generate_hourly_grouping,
					"calcs": [
						{"column_name": "hourly_median", "calc_type": "median"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([50.9517, 42.6859, 41.2875])
		},
		{
			"id": 8,
			"desc": "This is a test of the mean function for the hourly grouping",
			"config": {
				"interval": {
					"definition": ds.generate_hourly_grouping,
					"calcs": [
						{"column_name": "hourly_mean", "calc_type": "mean"}
					],
					"output_path": "doesnt matter...."
				}
			},
			"expected_result": np.array([50.9609333333, 42.6859, 39.7368])
		}
	]

	for test in tests:
		print(f"Running test # {test['id']}, \n {test['desc']}")

		# override the config
		ds.__grouping_config__ = test["config"]

		interval_df = dict()
		DataStream.calculate_interval_level_data(ds=ds,
		                                         grouping_configs=test["config"],
		                                         interval_df=interval_df)

		expected_column_name = test["config"]["interval"]["calcs"][0]["column_name"]

		result = np.around(interval_df["interval"]["df"][expected_column_name].values, 4)
		expected_result = np.around(test["expected_result"], 4)

		print("result:")
		print(result)
		print("expected:")
		print(expected_result)

		assert np.array_equal(result, expected_result) == True

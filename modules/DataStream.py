import numpy as np
import pandas as pd
import re
from typing import List
from config import __root_dir__, logger
from collections.abc import Callable


class DataStream():
	""" Class for data intake and manipulation """
	__stream_id__: int

	# Dataframe for the contents of the file
	__df__: pd.DataFrame

	# list of columns that should be present
	__valid_column_names__: List[str]

	__is_valid_stream__: bool
	__file_intake_error__: ValueError | None

	__total_intervals__: int | None
	__group_by__: List[str] | List[None]

	__grouping_config__: dict
	__calcs_config__: dict

	def __init__(self, stream_id: int, valid_column_names: List[str]):
		self.__stream_id__ = stream_id

		# list of columns which should be present on the csv
		self.__valid_column_names__ = valid_column_names

		# defaults
		self.__is_valid_stream__ = False
		self.__total_intervals__ = None
		self.__file_intake_error__ = None
		self.__group_by__ = []

		# List of groupings used, and calculations that should be ran
		# --------------------------------------------------------------------------------------------------------------------
		self.__grouping_config__ = {
			"day_interval": {
				"definition": self.generate_daily_grouping,
				"calcs": [{"column_name": "day_max", "calc_type": "max"},
				          {"column_name": "day_min", "calc_type": "min"},
				          {"column_name": "day_median", "calc_type": "median"},
				          {"column_name": "day_mean", "calc_type": "mean"}],
				"output_path": f"{__root_dir__}/Output/daily_interval_data.csv"
			},
			"hour_interval": {
				"definition": self.generate_hourly_grouping,
				"calcs": [{"column_name": "hour_max", "calc_type": "max"},
				          {"column_name": "hour_min", "calc_type": "min"},
				          {"column_name": "hour_median", "calc_type": "median"},
				          {"column_name": "hour_mean", "calc_type": "mean"}],
				"output_path": f"{__root_dir__}/Output/hourly_interval_data.csv"
			}
		}

		# mapping of calculations to function names
		self.__calcs_config__ = {
			"max": {
				"func": self.calculate_max,
				"args": ["value", self.get_group_by]
			},
			"min": {
				"func": self.calculate_min,
				"args": ["value", self.get_group_by]
			},
			"mean": {
				"func": self.calculate_mean,
				"args": ["value", self.get_group_by]
			},
			"median": {
				"func": self.calculate_median,
				"args": ["value", self.get_group_by]
			}
		}

	# ------------------------------------------------------------------------------------------------------------------

	# Get/Set funcs
	# --------------------------------------------------------------------------------------------------------------------
	def get_df(self):
		return self.__df__

	def get_file_intake_error(self):
		return self.__file_intake_error__

	def get_group_by(self):
		return self.__group_by__

	def get_grouping_config(self):
		return self.__grouping_config__

	def get_output_path(self, interval_type: str) -> str:
		return self.__grouping_config__[interval_type]["output_path"]

	def get_stream_id(self):
		return self.__stream_id__

	def get_total_intervals(self):
		return self.__total_intervals__

	def set_group_by(self, group_by):
		self.__group_by__ = group_by

	# --------------------------------------------------------------------------------------------------------------------

	# misc helper funcs
	# --------------------------------------------------------------------------------------------------------------------
	def is_valid_stream(self):
		return self.__is_valid_stream__

	def read_csv_data(self, file: str) -> bool:
		try:
			self.__df__ = pd.read_csv(file, usecols=self.__valid_column_names__, index_col=False).astype({
				"timestamp": "int64", "dttm_utc": "datetime64[ns]", "value": "float64",
				"estimated": "int64", "anomaly": "float64"
			})
			self.__total_intervals__ = len(self.__df__.index)

			if self.__total_intervals__ > 0:
				self.__is_valid_stream__ = True

				self.__df__["stream_id"] = self.__stream_id__

				# restructure the columns so that ID is the first column (just so that the data looks a bit better)
				cols = self.__df__.columns.tolist()
				cols = cols[-1:] + cols[:-1]
				self.__df__ = self.__df__[cols]

		except ValueError as err:
			self.__df__ = pd.DataFrame()
			self.__is_valid_stream__ = False
			self.__file_intake_error__ = err

		return self.__is_valid_stream__

	# --------------------------------------------------------------------------------------------------------------------

	# Calculation member functions
	# --------------------------------------------------------------------------------------------------------------------
	def calculate_max(self, operation_field: str, group_by: Callable) -> pd.DataFrame:
		return self.__df__.groupby(group_by())[operation_field].agg(["max"])

	def calculate_min(self, operation_field: str, group_by: Callable) -> pd.DataFrame:
		return self.__df__.groupby(group_by())[operation_field].agg(["min"])

	def calculate_mean(self, operation_field: str, group_by: Callable) -> pd.DataFrame:
		return self.__df__.groupby(group_by())[operation_field].agg(["mean"])

	def calculate_median(self, operation_field: str, group_by: Callable) -> pd.DataFrame:
		return self.__df__.groupby(group_by())[operation_field].agg(["median"])

	# --------------------------------------------------------------------------------------------------------------------

	# Grouping functions
	# --------------------------------------------------------------------------------------------------------------------
	def generate_daily_grouping(self) -> None:
		self.__df__["dttm_utc"] = pd.to_datetime(self.__df__["dttm_utc"])
		self.__df__["day_interval"] = self.__df__["dttm_utc"].dt.date

		self.set_group_by(["day_interval"])

	def generate_hourly_grouping(self) -> None:
		self.__df__["dttm_utc"] = pd.to_datetime(self.__df__["dttm_utc"])
		self.__df__["day_interval"] = self.__df__["dttm_utc"].dt.date
		self.__df__["hour_interval"] = self.__df__["dttm_utc"].dt.hour

		self.set_group_by(["day_interval", "hour_interval"])

	# --------------------------------------------------------------------------------------------------------------------

	def get_interval_level_results(self, grouping_type: str, grouping_config: dict) -> pd.DataFrame:

		result: pd.DataFrame = False

		# call the configured function to generate the column(s) that the data will be grouped by
		grouping_config["definition"]()

		for gc in grouping_config["calcs"]:
			column_name = gc["column_name"]
			calc_type = gc["calc_type"]

			if result is False:
				# call the registered function, and unpack the registered arguments
				result = self.__calcs_config__[calc_type]["func"](*self.__calcs_config__[calc_type]["args"])
				result = result.rename(columns={result.columns[0]: column_name})
			else:
				tmp = self.__calcs_config__[calc_type]["func"](*self.__calcs_config__[calc_type]["args"])
				tmp = tmp.rename(columns={tmp.columns[0]: column_name})
				result = result.join(tmp)

		result = result.reset_index()
		result["stream_id"] = self.__stream_id__

		# restructure the columns so that ID is the first column (just so that the data looks a bit better)
		cols = result.columns.tolist()
		cols = cols[-1:] + cols[:-1]
		result = result[cols]
		return result

	def get_stream_level_results(self) -> pd.DataFrame:
		stream_response = {"stream_id": [self.get_stream_id()],
		                   "status": [""],
		                   "message": [None],
		                   "rank": [np.nan],
		                   "% of 0 and NaN": [np.nan],
		                   "% of 0": [np.nan],
		                   "% of NaN": [np.nan],
		                   "count of 0 and NaN": [np.nan],
		                   "count of 0's": [np.nan],
		                   "count of NaN": [np.nan],
		                   "ignore": [True]}

		file_intake_error = self.get_file_intake_error()
		if file_intake_error is not None:
			stream_response["status"] = "Error"
			stream_response['message'] = repr(file_intake_error)
			return pd.DataFrame(data=stream_response)

		total_intervals = self.get_total_intervals()
		if total_intervals == 0:
			stream_response["status"] = "Warning"
			stream_response["message"] = ["Empty file - Structure is correct but has no data"]
			return pd.DataFrame(data=stream_response)

		counts = self.__df__["value"].value_counts(dropna=False).reset_index()
		count_of_zero = counts.loc[counts["index"] == 0, "value"].sum()
		count_of_nan = counts.loc[np.isnan(counts["index"]) == True, "value"].sum()
		count_of_one = counts.loc[counts["index"] == 1, "value"].sum()

		stream_response["status"] = ["Processed"]
		stream_response["% of 0 and NaN"] = [round(((count_of_nan + count_of_zero) / total_intervals), 4)]
		stream_response["% of 0"] = [round((count_of_zero / total_intervals), 4)]
		stream_response["% of NaN"] = [round((count_of_nan / total_intervals), 4)]
		stream_response["count of 0 and NaN"] = [count_of_zero + count_of_nan]
		stream_response["count of 0's"] = [count_of_zero]
		stream_response["count of NaN"] = [count_of_nan]
		stream_response["ignore"] = [(True if (count_of_nan + count_of_zero + count_of_one) == total_intervals else
		                              False)]
		self.__is_valid_stream__ = False if (count_of_nan + count_of_zero + count_of_one) == total_intervals else True

		return pd.DataFrame(data=stream_response)


def get_data_stream(stream_id: int, file_path: str, valid_column_names: List[str]) -> DataStream:
	ds = DataStream(stream_id, valid_column_names)
	ds.read_csv_data(file=file_path)

	return ds


def get_stream_id(pattern: str, file: str) -> int:
	return int(re.findall(pattern=pattern, string=file)[0])


def calculate_interval_level_data(ds: DataStream, grouping_configs: dict, interval_df: dict) -> None:
	for grouping_type, grouping_config in grouping_configs.items():
		logger.info(f"Getting the data for the grouping type: {grouping_type}")

		if grouping_type not in interval_df:
			"""each grouping_type will have its own index, and will have its own output_path; define it only when it 
			isn't present"""
			interval_df[grouping_type] = {"df": pd.DataFrame(), "output_path": ds.get_output_path(grouping_type)}

		interval_level_res = ds.get_interval_level_results(grouping_type, grouping_config)
		interval_df[grouping_type]["df"] = interval_df[grouping_type]["df"].append(interval_level_res, ignore_index=True)


# TODO remove redundant function - call get_stream_level_results directly from ds, and pass stream_df to it
def calculate_stream_level_data(ds: DataStream, stream_df: pd.DataFrame) -> pd.DataFrame:
	return stream_df.append(ds.get_stream_level_results(), ignore_index=True)

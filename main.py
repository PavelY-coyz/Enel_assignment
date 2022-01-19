# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import config
import pandas as pd
import datetime
from modules import csv, smtp
from modules.DataStream import calculate_interval_level_data, calculate_stream_level_data, get_data_stream, \
	get_stream_id


def main():
	# init ---------------------------------------------------------------------------------------------------------------
	path = config.csv_path
	pattern = config.csv_pattern
	logger = config.logger

	logger.info("Process Started \n\n")
	start_time = datetime.datetime.now()
	# --------------------------------------------------------------------------------------------------------------------

	try:
		file_list = csv.get_list_of_files(path, pattern)
	except NotADirectoryError as e:
		return smtp.send_email_notification(level="Critical", message=repr(e))

	if len(file_list) == 0:
		return smtp.send_email_notification(level="Warning", message="No files found")

	# initialize summary objects for stream/interval level data
	stream_df = pd.DataFrame()
	interval_df = dict()

	for file in file_list:
		stream_id = get_stream_id(pattern=config.stream_id_pattern, file=file)

		logger.info(f"Start processing Stream(ID): {stream_id}")

		# get DataStream object for the file
		ds = get_data_stream(stream_id=stream_id,
		                     file_path=f"{path}{file}",
		                     valid_column_names=config.valid_column_names)

		logger.info(f"Getting the stream-level data and classifications for Stream(ID): {stream_id}")

		# TODO - call get_stream_level_results, and assign to temp DF. Check for ignore, and then append to main df.
		stream_df = calculate_stream_level_data(ds=ds,
		                                        stream_df=stream_df)

		if ds.is_valid_stream() is not True:
			logger.info(f"Since stream is invalid - skipping the interval-level calculations, and going to the next file")
			continue

		logger.info(f"Getting the interval-level data/calculations for Stream(ID): {stream_id})")
		calculate_interval_level_data(ds=ds,
		                              grouping_configs=ds.get_grouping_config(),
		                              interval_df=interval_df)

		logger.info(f"Finished processing Stream(ID): {stream_id}")

	# calculate the dense rank for records that aren't being ignored
	stream_df["rank"] = stream_df.loc[stream_df["ignore"] == False]["count of 0 and NaN"].rank(ascending=False,
	                                                                                           method="dense").astype(
																																																							"int32")
	# store output
	logger.info(f"Writing Stream - summary - DataFrame to CSV")
	stream_df.to_csv(config.output_stream_path, index=False)

	for interval_type, df in interval_df.items():
		logger.info(f"Writing Interval- summary -  DataFrame of type : {interval_type} - to CSV")
		df["df"].to_csv(df["output_path"], index=False)

	logger.info("Process Ended \n\n")
	end_time = datetime.datetime.now()
	logger.info(f"Duration: {(end_time-start_time).total_seconds()}")


if __name__ == '__main__':
	main()

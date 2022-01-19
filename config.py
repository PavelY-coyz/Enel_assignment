import os
import logging
import logging.config
import datetime

__env__ = "DEV"

__root_dir__ = os.getcwd().replace('\\', '/')
__logs_dir__ = f"{__root_dir__}/logs/"

# input file - valid column names
valid_column_names = ["timestamp", "dttm_utc", "value", "estimated", "anomaly"]

# input paths
csv_path = f"{__root_dir__}/all-data.tar/csv/"
csv_pattern = r"^[1-9]+[0-9]*\.csv$"
csv_path_test = f"{__root_dir__}/tests/files/csv/"
stream_id_pattern = r"^([1-9]+[0-9]*)\.csv$"

# output paths
output_stream_path = f"{__root_dir__}/Output/stream_level_data.csv"

# logging
# ######################################################################################################################
# Prevent logging object from being recreated on each call to config; same logger configuration persists
if not logging.getLogger('Enel').hasHandlers():
	__current_date__ = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

	logger = logging.getLogger('Enel')
	logger.setLevel(logging.DEBUG)

	# console handler
	__ch__ = logging.StreamHandler()
	__ch__.setLevel(logging.DEBUG)

	# file handler
	__fh__ = logging.FileHandler(filename=f"{__logs_dir__}{__current_date__}.log", mode="a")
	if __env__ == "DEV":
		__fh__.setLevel(logging.DEBUG)
	else:
		__fh__.setLevel(logging.INFO)
	# -----------------------------------

	# create formatter
	__formatter__ = logging.Formatter(
		'%(asctime)s || %(name)s || %(levelname)s || %(module)s || %(lineno)d || %(message)s')
	__formatter__.datefmt = "%Y-%m-%d,%H:%M:%S"

	# add formatter to console handler
	__ch__.setFormatter(__formatter__)
	__fh__.setFormatter(__formatter__)

	# TODO add SMTP handler for CRITICAL msgs and/or SQLAlchemy

	# add handlers to logger
	logger.addHandler(__ch__)
	logger.addHandler(__fh__)

	del __ch__, __fh__, __formatter__, __current_date__
else:
	logger = logging.getLogger('Enel')
# End Logging ##########################################################################################################

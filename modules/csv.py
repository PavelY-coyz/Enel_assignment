# named csv_local because you cannot import Pandas into a module called "csv"

import os
import re
from config import logger as logger


def get_list_of_files(path: str, pattern: str) -> list[str]:
	if not os.path.isdir(path):
		logger.critical(f"Invalid Directory Path : '{path}'")
		raise NotADirectoryError(f"Invalid Directory Path : {path}")

	file_list = []

	for f in os.listdir(path):
		if re.match(pattern, f):
			file_list.append(f)

	if len(file_list) == 0:
		logger.warning("No files found")
	else:
		logger.info(f"{len(file_list)} file(s) found with the pattern '{pattern}' in path {path} ")

	return file_list

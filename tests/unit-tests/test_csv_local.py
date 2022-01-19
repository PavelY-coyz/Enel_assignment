import pytest
import config as conf
from modules import csv


def test_get_list_of_files_test_path():
	path = f"{conf.csv_path_test}csv_local_test/"
	pattern = conf.csv_pattern

	files = csv.get_list_of_files(path, pattern)
	assert type(files) == list
	assert len(files) == 3


def test_get_list_of_files():
	"""Just make sure it returns a list; otherwise the prod directory doesn't exist"""
	path = conf.csv_path
	pattern = conf.csv_pattern

	files = csv.get_list_of_files(path, pattern)
	assert type(files) == list


def test_get_list_of_files_invalid_path():
	path = "something random and invalid"
	pattern = conf.csv_pattern
	try:
		files = csv.get_list_of_files(path, pattern)
	except Exception as e:
		files = e
	finally:
		assert type(files) == NotADirectoryError








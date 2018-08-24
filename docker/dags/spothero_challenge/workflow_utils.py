import shutil
import hashlib
import os
import requests
from uuid import uuid4


def fetch_file(url, path):
	response = requests.get(url, stream=True)
	with open(path, "wb+") as f:
		shutil.copyfileobj(response.raw, f)
		f.flush()

def _md5(path):
	signature = hashlib.md5()
	with open(path, "rb") as f:
		for l in f:
			signature.update(l)
	return signature.hexdigest()

def atomic_swap(stage_file_path, data_file_path):
	"""
	Returns boolean; True=data was modified, False=data was not modified
	"""
	matches = os.path.exists(data_file_path) and _md5(stage_file_path) == _md5(data_file_path)
	if matches:
		# We don't need the new staged data since it is identical to the previous data
		os.remove(stage_file_path)
		return False
	else:
		# We do need the new staged data, atomically rename it to the final destination
		os.rename(stage_file_path, data_file_path)
		return True

class WorkflowDataFetcher:
	def __init__(self, workflow_dir):
		self._workflow_dir = workflow_dir
		self._stage_dir = os.path.join(workflow_dir, ".stage")

		try:
			os.mkdir(self._workflow_dir)
		except FileExistsError:
			pass

		try:
			os.mkdir(self._stage_dir)
		except FileExistsError:
			pass

	def _get_data_path(self, dataset_name):
		return os.path.join(self._workflow_dir, dataset_name)

	def fetch(self, fetch_func, dataset_name):
		"""
		Returns tuple (output, did_change);
		where output is the return value of the fetch_func
		where did_change indicates whether the underlying datasource changed
		"""
		stage_path = uuid4().hex
		final_path = self._get_data_path(dataset_name)
		fetch_func(stage_path)
		did_change = atomic_swap(stage_path, final_path)
		return did_change
import shutil
import os
import requests
from uuid import uuid4


# Utils for pulling down files from imdb
def fetch_file(url, path):
    response = requests.get(url, stream=True)
    with open(path, "wb+") as f:
        shutil.copyfileobj(response.raw, f)
        f.flush()


def fetch_etag(url):
    response = requests.head(url)
    ETag = response.headers.get("ETag", None)
    return ETag


# For writing to a file atomically
def _atomic_write(tmp_file_path, final_file_path, func):
    with open(tmp_file_path, "w+") as tmpfile:
        func(tmpfile)

    os.rename(tmp_file_path, final_file_path)


def _get_contents(path):
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return None


class WorkflowDataManager:
    """
    Little utility to refer to files by names instead of file paths
    Internally, the names get translated to file paths
    NOTE: no encoding of dataset names is done since this is a POC
    do not use filesystem-unsafe chars

    """
    def __init__(self, workflow_dir):
        self._workflow_dir = workflow_dir

    @property
    def _stage_dir(self):
        return os.path.join(self._workflow_dir, ".stage")

    @property
    def _cache_dir(self):
        return os.path.join(self._workflow_dir, "cache_tags")

    def _get_stage_path(self):
        return os.path.join(self._stage_dir, uuid4().hex)

    def _get_cache_path(self, dataset_name):
        return os.path.join(self._cache_dir, dataset_name)

    def get_data_path(self, dataset_name):
        return os.path.join(self._workflow_dir, dataset_name)

    def update_cache_tag(self, dataset_name, current_cache_tag, did_change_func=lambda: None, did_not_change_func=lambda: None):
        cache_path = self._get_cache_path(dataset_name)
        last_cache_tag = _get_contents(cache_path)
        did_change = last_cache_tag in (None, "") or current_cache_tag in (None, "") or current_cache_tag != last_cache_tag

        if did_change:
            did_change_func()

            # commit the cache tag after the callback
            stage_path = self._get_stage_path()
            _atomic_write(stage_path, cache_path, lambda tmpfile: tmpfile.write(current_cache_tag))
        else:
            did_not_change_func()

        return did_change

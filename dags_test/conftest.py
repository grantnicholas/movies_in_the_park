import os
import pytest

RESOURCES_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "resources")


@pytest.fixture
def get_resource_file():
    return lambda path: os.path.join(RESOURCES_DIR, path)


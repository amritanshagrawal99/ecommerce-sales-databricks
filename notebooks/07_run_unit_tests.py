# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Run the unit tests on Databricks
# MAGIC Runs the same pytest suite used locally against the cluster's Spark session.
# MAGIC Use a single-user (dedicated) cluster: the ingestion tests write small temporary files to the driver's
# MAGIC local disk. The integration tests use `data/raw/*` from the repo.

# COMMAND ----------

# MAGIC %pip install pytest openpyxl --quiet

# COMMAND ----------

import os
import sys

import pytest

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.dont_write_bytecode = True  # workspace files are read-only for .pyc
exit_code = pytest.main([os.path.join(repo_root, "tests"), "-v", "-p", "no:cacheprovider",
                         "--rootdir", repo_root, "-c", os.path.join(repo_root, "pytest.ini")])
assert exit_code == 0, f"pytest failed with exit code {exit_code}"

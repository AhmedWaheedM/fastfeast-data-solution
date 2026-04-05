from FastFeast.pipeline.config.metadata import load
from FastFeast.support.logger import pipeline as log
from FastFeast.pipeline.config.config import get_config
from pathlib import Path
import os

#Call load function to return data from yaml file
config = get_config()

yaml_path = Path(__file__).parent.parent / config.paths.metadata_yaml
metadata_settings = load(yaml_path)

batch = metadata_settings.batch
stream = metadata_settings.stream
############################################
#file name for test

file_name = "cities.json"

############################################
def get_expected_list(file_name, type):
    """
    - Get all columns of passed file from metadata, ase expected columns
    """
    source = batch if type == 'batch' else stream

    for file_meta in source:
        if file_meta.file_name == file_name:
            return [col.name for col in file_meta.columns]

    return None 

#############################################

def get_actual_files(base_path):
    """
    return copied files from our distnation
    """
    #print("hello get_files")
    return [name for name in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, name))]


#################################################################

from collections import Counter
def compare_lists(actual_list, expected_list):
    """
    **actual_list** : list of copied files, which we get from the source
    **expected_list** : list of files we have in metadata

    **About Function** : (generic function for files and columns) takes 2 lists, compare their values, and return their cases
      - missed: if there is missed file or column
      - extra: if there is extra file or cokumn
      - duplicte: if there id duplicaed column
      - wanted: returns our wanted files and columns
    """

    actual_set = set(actual_list)
    expected_set = set(expected_list)

    return {
        "missed": list(expected_set - actual_set),
        "extra": list(actual_set - expected_set),
        "duplicated": [col for col, count in Counter(actual_list).items() if count > 1],
        "wanted_columns": list(actual_set & expected_set)
        }


#############################################

#our entry point
def compare_files(folder_path, pipeline_type = 'batch'):
  """
  - compare actual with expected and return our wanted list
  - log if there is unexpected result
  """
  expected_list = get_expected_list(file_name,pipeline_type)
  actual_list = get_actual_files(folder_path)
  results = compare_lists(actual_list, expected_list)
  if results["missed"]:
    #logging(f"Files missed: {', '.join(results['missed'])}")
    print("MISSEDDDDDDDDDDDDDDDDDDDD")
    return False
  if results["extra"]:
     print("EXTRAAAAAAAAAAAAAAAAAA")
    #logging(f"Files extra: {', '.join(results['extra'])}")
  if results["wanted_files"]:
    return results["wanted_files"]
  
#############################################
def compare_columns(pyarrow_table, pipeline_type = 'batch'):
  expected_list = get_expected_list(file_name,pipeline_type)

  # for in this directory to get all files of it
  actual_list = list(pyarrow_table.column_names)
  print("Actual list",actual_list)
  print("Expected list", expected_list)
  results = compare_lists(actual_list, expected_list)
  if results["missed"]:
    #log(f"Columns missed: {', '.join(results['missed'])}")
    print("log missedddddddddddddddd")
    return False
  if results["extra"]:
    #drop
    #logging(f"Columns extra: {', '.join(results['extra'])}")
        print("log extraaaaaaaaaaaaaaaa")

  if results["duplicated"]:
    #logging(f"Columns duplicated: {', '.join(results['duplicated'])}")
    #drop
        print("log duplicateddddddddddddddd")
  if results["wanted_columns"]:
    print("YEEEEEEEEEEEEEEEEEEES")
    return True

#############################################
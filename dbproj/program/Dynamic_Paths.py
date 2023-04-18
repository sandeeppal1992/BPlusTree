import os
import dbproj


proj_dir = os.path.dirname(os.path.abspath(dbproj.__file__))
# print(proj_dir)

DATA_DIR = proj_dir + "/data/"
INDEX_DIR = proj_dir + "/index/"
OUTPUT_DIR = proj_dir + "/queryOutput/"
TREE_PIC_DIR = proj_dir + "/treePic/"

SCHEMA_FILE = "schemas.txt"
INDEX_DIR_FILE = "directory.txt"
PAGE_POOL_FILE = "pagePool.txt"
PAGE_LINK_FILE = "/pageLink.txt"
QUERY_RSLT_FILE = "/queryResult.txt"

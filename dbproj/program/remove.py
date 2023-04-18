"""
    remove.py includes removeTree(*) and removeTable(*)
    Run build(*) on the provided data set, and create two B+_trees with an order of 2, one on Suppliers.sid, and
    the other on Supply.pid.
"""
import json
import shutil
from dbproj.program.Dynamic_Paths import *

class RemoveElements:
    def __init__(self):
        self.index_dir = INDEX_DIR
        self.index_file = INDEX_DIR_FILE
        self.tree_pic_dir = TREE_PIC_DIR
        self.data_dir = DATA_DIR
        self.schema_file = SCHEMA_FILE
        self.pg_lnk_file = PAGE_LINK_FILE
        self.pg_pool_file = PAGE_POOL_FILE
        self.leaf_type_pos = 0
        self.rel_pos = 0
        self.attr_pos = 1
        self.root_pos = 2
        self.contnt_pos = 2

    def seq_search(self, filename):
        with open(os.path.join(self.index_dir, filename)) as f:
            content = f.readlines()[0]
            data = json.loads(content)
            if data[self.leaf_type_pos] == "I":
                pages = []
                for entry in data[self.contnt_pos]:
                    if entry.endswith(".txt"):
                        self.seq_search(entry)
                        pages.append(entry)
                        os.remove(os.path.join(self.index_dir, entry))

                with open(os.path.join(self.index_dir, self.pg_pool_file)) as df:
                    page_pool = json.loads(df.readlines()[0])
                    page_pool.extend(pages)
                    page_pool.sort(reverse=True)
                with open(os.path.join(self.index_dir, self.pg_pool_file), 'w') as df:
                    df.write(json.dumps(page_pool))

    def removeTree(self, rel, att):
        with open(self.index_dir + self.index_file) as f:
            fdata = f.readlines()[0]
            tuples = json.loads(fdata)

            for tuple_ in tuples:
                if tuple_[self.rel_pos] == rel and tuple_[self.attr_pos] == att:
                    self.seq_search(tuple_[self.root_pos])
                    os.remove(self.index_dir + tuple_[self.root_pos])

        with open(os.path.join(self.index_dir, self.index_file), "w") as f:
            res = json.dumps([tuple_ for tuple_ in tuples if tuple_[self.rel_pos] != rel or tuple_[self.attr_pos] != att])
            f.write(res)

    def removeTable(self, rel):
        path = self.data_dir + rel
        if os.path.exists(path):
            with open(self.data_dir + rel + self.pg_lnk_file) as pg_lnk:
                fdata = pg_lnk.readlines()[0]
                pages = json.loads(fdata)

            with open(self.data_dir + self.pg_pool_file) as pg_pl:
                fdata = pg_pl.readlines()[0]
                page_pool = json.loads(fdata)

            with open(self.data_dir + self.pg_pool_file, "w") as pg_pl:
                res = json.dumps(page_pool + pages)
                pg_pl.write(res)

            with open(self.data_dir + self.schema_file) as sc:
                fdata = sc.readlines()[0]
                fields = json.loads(fdata)

            fields = [field for field in fields if field[self.rel_pos] != rel]
            with open(self.data_dir + self.schema_file, "w") as sc:
                res = json.dumps(fields)
                sc.write(res)

            shutil.rmtree(path)
            with open(self.index_dir + self.index_file) as id_:
                indices = json.loads(id_.readlines()[0])
                for index in indices:
                    if index[self.rel_pos] == rel:
                        self.removeTree(index[self.rel_pos], index[self.attr_pos])


if __name__ == '__main__':
    obj = RemoveElements()

    ## Calling functions
    # obj.removeTable("Supply_tmp")
    # obj.removeTree("Suppliers", "sid")
    # obj.removeTree("Supply", "pid")
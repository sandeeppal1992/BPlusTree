"""
    display.py contains displayTree(*) and displayTable(*).
    Run displayTree(*) to display the structures of the two B+_trees you create by running buildTree.py to display:
    i) B+ tree on Suppliers on sid
    ii) B+ tree on Supply on pid
    They should be  displayed in files Suppliers_sid.txt and Supply_pid.txt, respectively, under folder treePic.
"""
from dbproj.program.Dynamic_Paths import *
from dbproj.program.displayHelper import *
from dbproj.program.Dynamic_Paths import *
import json

objDispHelper = DisplayHelper()

class Display:
    def __init__(self):
        self.tree_pic_dir = TREE_PIC_DIR
        self.data_dir = DATA_DIR
        self.schema_file = SCHEMA_FILE
        self.pg_lnk_file = PAGE_LINK_FILE
        self.out_dir = OUTPUT_DIR
        self.idx_dir = INDEX_DIR
        self.idx_file = INDEX_DIR_FILE
        self.rel_pos = 0
        self.leaf_type_pos = 0
        self.attr_pos = 1
        self.root_pos = 2
        self.order_pos = 3

    def get_node_val(self):
        dirpath = INDEX_DIR + INDEX_DIR_FILE
        with open(dirpath) as f:
            fdata = json.loads(f.read())
        if fdata:
            if len(fdata) == 1:
                raise Exception("Try running buildTree.py for both Suppliers.sid and Supply.pid!!")
            elif len(fdata) == 2:
                suppliers_node = [pg[2] for pg in fdata if pg[0] == 'Suppliers'][0]
                supply_node = [pg[2] for pg in fdata if pg[0] == 'Supply'][0]
        else:
            raise Exception("Try running buildTree.py first!!")
        return [suppliers_node, supply_node]

    def displayTree(self, fname):
        if fname == '*':
            pg_list = self.get_node_val()
            try:
                for node in pg_list:
                    att, rel = objDispHelper.get_rel_and_att(node)
                    tree_pic_name = objDispHelper.get_tree_name(rel, att)
                    indent = 0
                    with open(self.idx_dir + node) as root,\
                            open(self.tree_pic_dir + tree_pic_name, "w") as tree_pic:
                        fdata = root.read()
                        tree_pic.write(" " * indent + node + ": " + fdata + "\r\n")
                        data = json.loads(fdata)
                        if data[self.leaf_type_pos] == "I":
                            for entry in data[2]:
                                if entry.endswith(".txt"):
                                    objDispHelper.seq_search(entry, indent + 2, tree_pic)
                print("Trees built successfully inside directory 'treePic'")
            except Exception as e:
                return e
        else:
            raise Exception("displayTree(*) is expected")


    def displayTable(self, rel, fname):
        """
        :param rel: relation /schema
        :param fname: file name
        :return:
        """
        path = self.data_dir + rel
        with open(self.data_dir + rel + "/" + self.pg_lnk_file) as pl:
            content = pl.read()
            pages = json.loads(content)

        data = []
        for page in pages:
            with open(path + "/" + page) as f:
                content = f.read()
                two_tuples = json.loads(content)
                data += two_tuples

        if not os.path.exists(self.out_dir):
            os.mkdir(self.out_dir)
        with open(self.out_dir + fname, "a+") as qr:
            for d in data:
                qr.write(json.dumps(d) + "\r\n")
            qr.write("\r\n")


if __name__ == '__main__':
    obj = Display()
    obj.displayTree('*')

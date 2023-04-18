"""
    includes the build(*)
    Run build(*) on the provided data set, and create two B+_trees with an order of 2, one on Suppliers.sid, and
    the other on Supply.pid.
"""
from dbproj.program import BPlusTree
from dbproj.program.Dynamic_Paths import *
import json


class BuildTree:
    def __init__(self):
        self.index_dir = INDEX_DIR
        self.index_file = INDEX_DIR_FILE

    def build(self, rel, att, od):
        tuple_list = BPlusTree.get_all_pgs_attlst(rel, att)
        root = BPlusTree.Node(BPlusTree.get_page_pool_pages())
        root = BPlusTree.insert(root, tuple_list[0][0], tuple_list[0][1], od, root)
        for i in range(1, len(tuple_list)):
            key = tuple_list[i][0]
            next_index_point = tuple_list[i][1]
            root = BPlusTree.insert(BPlusTree.search_node_key(root, key)[0], key, next_index_point, od, root)
    
        root.write_to_node()
    
        with open(self.index_dir + self.index_file) as f:
            directory = json.loads(f.read())

        with open(self.index_dir + self.index_file, 'w') as f:
            directory_tree = []
            directory_tree.append(rel)
            directory_tree.append(att)
            directory_tree.append(root.node_page)
            directory.append(directory_tree)
            f.write(json.dumps(directory))
        # print(root.get_tree_struct())


if __name__ == '__main__':
    obj = BuildTree()

    ## Calling functions
    obj.build('Suppliers', 'sid', 2)
    obj.build('Supply', 'pid', 2)
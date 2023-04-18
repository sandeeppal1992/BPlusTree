import json
import math
from dbproj.program.Dynamic_Paths import *


class Node:

    def __init__(self, page):
        self.node_type = 'L'
        self.parent = None
        self.pointers = []
        self.node_page = page
        self.keys = []
        self.left = None
        self.right = None
        self.info = []
        self.index_dir = INDEX_DIR

    def write_to_node(self):
        node_data = []
        if self.node_type == 'I':
            for i in range(len(self.keys)):
                node_data.append(self.pointers[i].node_page)
                node_data.append(self.keys[i])
            node_data.append(self.pointers[len(self.pointers) - 1].node_page)
            self.info.append(self.node_type)
            if self.parent is not None:
                self.info.append(self.parent.node_page)
            else:
                self.info.append('nil')
            self.info.append(node_data)
        else:
            for i in range(len(self.keys)):
                node_data.append(self.keys[i])
                node_data.append(self.pointers[i])
            self.info.append(self.node_type)
            self.info.append(self.parent.node_page)
            if self.left is not None:
                self.info.append(self.left.node_page)
            else:
                self.info.append('nil')
            if self.right is not None:
                self.info.append(self.right.node_page)
            else:
                self.info.append('nil')
            self.info.append(node_data)

        with open(self.index_dir + self.node_page, 'w') as f:
            f.write(json.dumps(self.info))

        if self.node_type != 'L':
            for pointer in self.pointers:
                pointer.write_to_node()

    def get_tree_struct(self, level=0):
        ret = "\t" * level + repr(self.keys) + repr(self.pointers) + "\n"
        if self.node_type != 'L':
            for pointer in self.pointers:
                ret += pointer.get_tree_struct(level + 1)
        return ret


def get_all_pgs_attlst(rel, att):
    """
    :param rel:
    :param att:
    :return:
    """
    with open(DATA_DIR + SCHEMA_FILE) as f:
        fdata = f.read()
        schemas = json.loads(fdata)
    for relation_schema in schemas:
        if relation_schema[0] == rel and relation_schema[1] == att:
            att_index = relation_schema[3]

    try:
        response = []
        with open(DATA_DIR + rel + '/' + PAGE_LINK_FILE) as f:
            fdata = f.read()
            page_link = json.loads(fdata)
            for page in page_link:
                with open(DATA_DIR + rel + '/' + page) as pf:
                    page_content = pf.read()
                    records = json.loads(page_content)
                    for record in records:
                        response.append((record[att_index], page + '.' + str(records.index(record))))
        return response
    except:
        return []


def search_node_key(node, key):
    while node.node_type != 'L':
        length = len(node.keys)

        if key < node.keys[0]:
            node = node.pointers[0]
        elif key >= node.keys[length - 1]:
            node = node.pointers[length]
        else:
            for i in range(length - 1):
                if node.keys[i] <= key < node.keys[i + 1]:
                    node = node.pointers[i + 1]
                    break
    length = len(node.keys)
    index = None
    for i in range(length):
        if node.keys[i] == key:
            index = i
    return node, index


def insert(node, key, next_index_point, order, root):
    """
    :param node:
    :param key:
    :param next_index_point:
    :param order: order of tree
    :param root: root node
    :return:
    """
    upper_bound = order
    if node.parent is not None:
        lower_bound = order
        upper_bound = 2 * order

    append_key(node, key, next_index_point)

    if len(node.keys) > upper_bound:
        split_res = split_node(node)
        if node.parent is None:
            root = Node(get_page_pool_pages())
            root.node_type = 'I'
            node.parent = root
            split_res[0].parent = root
            root.pointers.append(node)
            insert(root, split_res[1], split_res[0], order, root)
        else:
            split_res[0].parent = node.parent
            root = insert(node.parent, split_res[1], split_res[0], order, root)

    return root

def append_key(node, key, pointer):
    check = False
    if key not in node.keys:
        check = True
        node.keys.append(key)
        node.keys.sort()
    index = node.keys.index(key)
    if node.node_type != 'L':
        index += 1
        node.pointers.insert(index, pointer)
    else:
        if check:
            node.pointers.insert(index, [pointer])
        else:
            node.pointers[index].append(pointer)


def split_node(node):
    right = Node(get_page_pool_pages())
    right.node_type = node.node_type
    total_keys = node.keys
    total_pointers = node.pointers
    node.keys = total_keys[0:int(math.floor(len(total_keys) / 2))]
    node.pointers = total_pointers[0:int(math.floor(len(total_pointers) / 2))]
    if node.node_type == 'L':
        right.keys = total_keys[int(math.floor(len(total_keys) / 2)):len(total_keys)]
        right.pointers = total_pointers[int(math.floor(len(total_pointers) / 2)):len(total_pointers)]
        node.right = right
        right.left = node
    else:
        right.keys = total_keys[int(math.floor(len(total_keys) / 2)) + 1:len(total_keys)]
        right.pointers = total_pointers[int(math.floor(len(total_pointers) / 2)):len(total_pointers)]
    if node.node_type != 'L':
        for i in range(len(right.pointers)):
            right.pointers[i].parent = right
    return right, total_keys[int(math.floor(len(total_keys) / 2))]


def get_page_pool_pages():
    pgpoolpath = INDEX_DIR + PAGE_POOL_FILE
    with open(pgpoolpath) as f:
        pgpool = json.loads(f.read())
        if pgpool:
            page = pgpool.pop(0)

    with open(pgpoolpath, 'w') as f:
        res = json.dumps(pgpool)
        f.write(res)
    return page



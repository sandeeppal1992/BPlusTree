from dbproj.program.Dynamic_Paths import *
import json
import os

LEAF_NODE = {
    "NODE_TYPE": 0,
    "PARENTAL_POINTER": 1,
    "LEFT_POINTER": 2,
    "RIGHT_POINTER": 3,
    "CONTENT": 4
}

INDEX_TYPE = {
    "CLUSTERED_INDEX": 0,
    "UNCLUSTERED_INDEX": 1
}

ATT_CATEGORY = {
    "IDS": 0,
    "NONIDS": 1
}

DIRECTION = {
    "LEFT": 0,
    "RIGHT": 1
}
ATT_TYPE = {
    "STRING": "str",
    "INTEGER": "int"
}

att_type = {"sid": ATT_TYPE['STRING'], "sname": ATT_TYPE['STRING'], "address": ATT_TYPE['STRING'],
            "pid": ATT_TYPE['STRING'], "pname": ATT_TYPE['STRING'], "color": ATT_TYPE['STRING'],
            "cost": ATT_TYPE['INTEGER']}

idx_clustered = [("Products", "pid"),
                 ("Suppliers", "sid"),
                 ("Supply", "sid")]

idx_unclustered = [
    ("Products", "pname"), ("Products", "color"),
    ("Suppliers", "sname"), ("Suppliers", "address"),
    ("Supply", "pid"), ("Supply", "cost")
]

ids_att = ["sid", "pid"]

class relAlgHelper:
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
        self.order_pos = 3
        self.max_len = 2

    def relation_schema(self, rel):
        with open(self.data_dir + self.schema_file) as files:
            file_line = files.readlines()[0]
            fields = json.loads(file_line)
            fields = [field for field in fields if field[0] == rel]

            def sort_position(x):
                return x[self.order_pos]

            fields.sort(key=sort_position)

            schema = [field[1] for field in fields]
        return schema

    def id_conv_string_to_int(self, val):
        return int(val[1:])

    def get_single_tuple(self, filename, index, rel, res):
        with open(self.data_dir + rel + "/" + filename) as f:
            fdata = f.readlines()[0]
            data = json.loads(fdata)
            res.append(data[index])
        return res

    def get_data_files(self, rel, node_content, res):
        for item in node_content:
            if isinstance(item, list):
                for pointer in item:
                    filename, index = pointer[:-2], int(pointer[-1])
                    with open(self.data_dir + rel + filename) as f:
                        fdata = f.readlines()[0]
                        data = json.loads(fdata)
                        res.append(data[index])
        return res

    def search(self, pointer, rel, res, direction=DIRECTION["LEFT"]):
        with open(self.index_dir + pointer) as f:
            node = f.readlines()[0]
            data = json.loads(node)
            content = data[LEAF_NODE["CONTENT"]]
            if direction == DIRECTION["LEFT"]:
                content.reverse()
            res = self.get_data_files(rel, content, res)
            next_pointer = data[
                LEAF_NODE["LEFT_POINTER"] if direction == DIRECTION["LEFT"] else LEAF_NODE["RIGHT_POINTER"]]

        if next_pointer != "nil":
            res = self.search(next_pointer, rel, res, direction)
        return res

    def clustered_idx_tuples(self, leaf_node, rel, val, op):
        res = []
        content = leaf_node[LEAF_NODE["CONTENT"]]
        counter = 0
        related_list = []
        for index, value in enumerate(content):
            if isinstance(value, str) and value == val:
                related_list = content[index + 1]

        if not related_list:
            raise Exception("Invalid val detected!! Please check your input value!!")

        with open(self.data_dir + rel + self.pg_lnk_file) as pl:
            content = pl.readlines()[0]
            pages = json.loads(content)

            if op == '=':
                for pointer in related_list:
                    filename, index = pointer[:-2], int(pointer[-1])
                    res = self.get_single_tuple(filename, index, rel, res)
            elif op == "<=":
                filename, index = related_list[-1][:-2], int(related_list[-1][-1])
                for idx, val in enumerate(pages):
                    counter += 1
                    if val == filename:
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            res.append(data[0])
                            if index == 1:
                                res.append(data[1])

                        break

                    with open(self.data_dir + rel + val) as f:
                        fdata = f.readlines()[0]
                        data = json.loads(fdata)
                        res += data
            elif op == '<':
                filename, index = related_list[0][:-2], int(related_list[0][-1])
                for idx, val in enumerate(pages):
                    counter += 1
                    if val == filename:
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            if index == 1:
                                res.append(data[0])

                        break

                    with open(self.data_dir + rel + val) as f:
                        fdata = f.readlines()[0]
                        data = json.loads(fdata)
                        res += data
            elif op == ">=":
                filename, index = related_list[0][:-2], int(related_list[0][-1])
                flag = False
                for idx, val in enumerate(pages):
                    counter += 1
                    if flag:
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            res += data

                    if val == filename:
                        flag = True
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            if index == 0:
                                res.append(data[0])

                            res.append(data[1])
            elif op == '>':
                filename, index = related_list[-1][:-2], int(related_list[-1][-1])
                flag = False
                for idx, val in enumerate(pages):
                    counter += 1
                    if flag:
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            res += data

                    if val == filename:
                        flag = True
                        with open(self.data_dir + rel + val) as f:
                            fdata = f.readlines()[0]
                            data = json.loads(fdata)
                            if index == 0:
                                res.append(data[1])
            else:
                raise Exception('Invalid op value!!!')
        return res, counter

    def unclustered_idx_tuples(self, leaf_node, rel, val, op):
        res = []
        counter = 0
        content = leaf_node[LEAF_NODE["CONTENT"]]
        if op in ('<', '<='):
            content.reverse()
            for index, value in enumerate(content):
                counter += 1
                if (isinstance(value, str) and value == val and op == '<=') or (isinstance(value, str) and value < val):
                    item = content[index - 1]
                    for pointer in item:
                        filename, index = pointer[:-2], int(pointer[-1])
                        res = self.get_single_tuple(filename, index, rel, res)
            if leaf_node[LEAF_NODE["LEFT_POINTER"]] != "nil":
                res = self.search(leaf_node[LEAF_NODE["LEFT_POINTER"]], rel, res, DIRECTION["LEFT"])
        elif op in ('>', '>='):
            for index, value in enumerate(content):
                counter += 1
                if (isinstance(value, str) and value == val and op == '>=') or (isinstance(value, str) and value > val):
                    item = content[index + 1]
                    for pointer in item:
                        filename, index = pointer[:-2], int(pointer[-1])
                        res = self.get_single_tuple(filename, index, rel, res)
            if leaf_node[LEAF_NODE["RIGHT_POINTER"]] != "nil":
                res = self.search(leaf_node[LEAF_NODE["RIGHT_POINTER"]], rel, res, DIRECTION["RIGHT"])
        elif op == '=':
            for index, value in enumerate(content):
                counter += 1
                if isinstance(value, str) and value == val:
                    item = content[index + 1]
                    for pointer in item:
                        filename, index = pointer[:-2], int(pointer[-1])
                        res = self.get_single_tuple(filename, index, rel, res)
        else:
            raise Exception('Invalid op value!!!')

        return res, counter

    def seq_search(self, fname, rel, val, op, index_type=INDEX_TYPE["CLUSTERED_INDEX"], att_type=ATT_CATEGORY["NONIDS"]):
        res = None
        counter = 1
        with open(self.index_dir + fname) as f:
            info = f.readlines()[0]
            data = json.loads(info)
            if data[self.leaf_type_pos] == "I":
                content = data[self.contnt_pos]
                located = False
                for index, value in enumerate(content):
                    if not value.endswith(".txt"):
                        if att_type == ATT_CATEGORY["IDS"]:
                            int_val, int_value = self.id_conv_string_to_int(val), self.id_conv_string_to_int(value)
                        input_less_than_value = int_val < int_value if att_type == ATT_CATEGORY["IDS"] else val < value
                        if input_less_than_value:
                            res, res_count = self.seq_search(content[index - 1], rel, val, op, index_type, att_type)
                            located = True
                            counter += res_count
                            break
                        elif val == value:
                            res, res_count = self.seq_search(content[index + 1], rel, val, op, index_type, att_type)
                            located = True
                            counter += res_count
                            break

                if not located:
                    res, res_count = self.seq_search(content[-1], rel, val, op, index_type, att_type)
                    counter += res_count
            else:
                counter = 0
                if index_type == INDEX_TYPE["CLUSTERED_INDEX"]:
                    res, res_count = self.clustered_idx_tuples(data, rel, val, op)
                    counter += res_count
                else:
                    res, res_count = self.unclustered_idx_tuples(data, rel, val, op)
                    counter += res_count

        return res, counter

    def update_schemas(self, rel, attList):
        new_items = []
        for index, value in enumerate(attList):
            new_items.append([rel, value, att_type[value], index])
        with open(self.data_dir + self.schema_file) as sc:
            fdata = sc.readlines()[0]
            schemas = json.loads(fdata)

        schemas += new_items
        with open(self.data_dir + self.schema_file, 'w') as f:
            f.write(json.dumps(schemas))

    def get_page(self):
        """
        :return:  page from page pool
        """
        with open(self.data_dir + self.pg_pool_file) as pp:
            content = pp.readlines()[0]
            page_pool = json.loads(content)
            if page_pool:
                page = page_pool.pop(0)
            else:
                raise Exception("Ran out of pages!")
        with open(self.data_dir + self.pg_pool_file, 'w') as f:
            f.write(json.dumps(page_pool))
        return page

    def write_to_pages(self, rel, res):
        rel_name = rel
        if os.path.exists(self.data_dir + rel):
            rel_name = rel + "_tmp"
            if os.path.exists(self.data_dir + rel_name):
                return
            os.mkdir(self.data_dir + rel_name)
        else:
            os.mkdir(self.data_dir + rel)
        page_link = []
        length = len(res)
        for i in range(0, length, self.max_len):
            page = self.get_page()
            page_link.append(page)
            with open(self.data_dir + rel_name + "/" + page, "w") as f:
                f.write(json.dumps(res[i:i + self.max_len]))

        with open(self.data_dir + rel_name + self.pg_lnk_file, "w") as pl:
            pl.write(json.dumps(page_link))

    def proj_rel_name(self, attList, rel):
        return rel + "_" + attList[0]

    def join_rel_name(self, rel1, rel2):
        return rel1 + "_" + rel2

    def join_by_index(self, rel, att, val):
        tree_root = None
        with open(self.index_dir + self.index_dir) as id_:
            indices = json.loads(id_.readlines()[0])
            for index in indices:
                if index[self.rel_pos] == rel and index[self.attr_pos] == att:
                    tree_root = index[self.root_pos]
                    break

        if att in ids_att:
            att_type = ATT_CATEGORY["IDS"]
        else:
            att_type = ATT_CATEGORY["NONIDS"]

        index_type = INDEX_TYPE["UNCLUSTERED_INDEX"]
        for ci in idx_clustered:
            if rel == ci[0] and att == ci[1]:
               index_type = INDEX_TYPE["CLUSTERED_INDEX"]
               break

        res, _ = self.seq_search(tree_root, rel, val, "=", index_type, att_type)
        return res


if __name__ == '__main__':
    pass
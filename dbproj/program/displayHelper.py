from dbproj.program.Dynamic_Paths import *
import json


class DisplayHelper:

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

    # @staticmethod
    def get_rel_and_att(self, fname):
        with open(self.idx_dir + self.idx_file) as idx:
            fdata = idx.read()
            tuples = json.loads(fdata)
            for tuple in tuples:
                if tuple[self.root_pos] == fname:  # position for root
                    rel = tuple[self.rel_pos]  # position for relation
                    att = tuple[self.attr_pos]  # position for attribute
                    break
        return att, rel

    def get_tree_name(self, rel, att):
        tree_name = rel + "_" + att + ".txt"
        return tree_name

    def schema_data_file(self, rel):
        with open(self.data_dir + self.schema_file) as files:
            file_line = files.read()
            fields = json.loads(file_line)
            fields = [field for field in fields if field[0] == rel]

            def sort_position(x):
                return x[self.order_pos]

            fields.sort(key=sort_position)
            schema = [field[1] for field in fields]
        return schema

    def display_schema(self, rel, filename):
        data = self.schema_data_file(rel)
        with open(self.out_dir + filename, "a+") as fw:
            fw.write(json.dumps(data) + "\r\n")

    def seq_search(self, fname, indent, distfile):
        with open(self.idx_dir + fname) as f:
            fdata = f.read()
            content = " " * indent + fname + ": " + fdata + "\r\n"
            distfile.write(content)
            data = json.loads(fdata)
            if data[self.leaf_type_pos] == "I":
                for entry in data[2]:  # content position
                    if entry.endswith(".txt"):
                        self.seq_search(entry, indent + 2, distfile)

    def get_schema_file_data(self):
        dirname = self.data_dir
        fname = dirname + self.schema_file
        schema_data = open(fname, "r").read()
        schema_data = json.loads(schema_data)
        return schema_data

    def get_rel_data_path(self, rel):
        dirname = self.data_dir
        return dirname + "/" + str(rel)

    def read_file(self, rel):
        dpath = self.get_rel_data_path(rel)
        pg_link_file = 'pageLink.txt'
        page_link_fpath = dpath + '/' + pg_link_file
        pglink_data = open(page_link_fpath, "r").read()
        pg_link_list = json.loads(pglink_data)
        self.get_pages_data_for_rel(dpath, pg_link_list)

    def get_pages_data_for_rel(self, dpath, pg_link_list):
        # print(dpath)
        pg_dict = {
            pages: open(dpath + "/" + str(pages), "r").read() for pages in pg_link_list
        }
        import pprint
        pprint.pprint(pg_dict)


if __name__ == '__main__':
    obj = DisplayHelper()
    obj.read_file('Supply')
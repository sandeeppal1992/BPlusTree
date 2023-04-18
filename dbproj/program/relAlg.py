"""
    includes the three relational algebra functions
"""
import pandas as pd
from dbproj.program.rel_Alg_Helper import *

objRelAlg = relAlgHelper()


class RelAlgebra:
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

    def sel(self, rel, att, op, val):
        """
            :param rel: relation (Products,Suppliers or Supply)
            :param att:attribute in relation (sid,pid, or any relation attribute)
            :param op: op is one of the five strings, ‘<’, ‘<=’, ‘=’, ‘>’, ‘>='
            :param val: value (search Term for selection)
            :return:
            example : select('Product', 'pname', '=', 'shovel')
        """
        counter = 0
        tree_root = None
        with open(self.index_dir + self.index_file) as id_:
            indices = json.loads(id_.read())
            for index in indices:
                if index[self.rel_pos] == rel and index[self.attr_pos] == att:
                    tree_root = index[self.root_pos]
                    break

        if att in ids_att:
            att_type = ATT_CATEGORY["IDS"]
        else:
            att_type = ATT_CATEGORY["NONIDS"]

        schema = objRelAlg.relation_schema(rel)

        data = []
        if tree_root:
            index_type = INDEX_TYPE["UNCLUSTERED_INDEX"]
            for ci in idx_clustered:
                if rel == ci[0] and att == ci[1]:
                    index_type = INDEX_TYPE["CLUSTERED_INDEX"]
                    break
            res, res_count = objRelAlg.seq_search(tree_root, rel, val, op, index_type, att_type)
            counter += res_count
            val = "With B+_tree, the cost of searching {att} {op} {val} on {rel} is {value} pages".format(rel=rel,
                                                                                                          att=att,
                                                                                                          op=op,
                                                                                                          val=val,
                                                                                                          value=counter)
            print(val)
        else:
            with open(self.data_dir + rel + self.pg_lnk_file) as pl:
                fdata = pl.read()
                pages = json.loads(fdata)
                res = []
                for page in pages:
                    counter += 1
                    with open(self.data_dir + rel + "/" + page) as pg:
                        page_content = pg.read()
                        page_data = json.loads(page_content)
                        data += page_data
                        df = pd.DataFrame(data, columns=schema)
                        if op == '<':
                            df = df.loc[df[att] < val]
                        elif op == '<=':
                            df = df.loc[df[att] <= val]
                        elif op == '=':
                            df = df.loc[df[att] == val]
                        elif op == '>':
                            df = df.loc[df[att] > val]
                        elif op == '>=':
                            df = df.loc[df[att] >= val]
                        else:
                            raise Exception('Invalid op value!!!')
                        res = df.values.tolist()

            val = "Without B+_tree, the cost of searching {att} {op} {val} on {rel} is {value} pages".format(rel=rel,
                                                                                                             att=att,
                                                                                                             op=op,
                                                                                                             val=val,
                                                                                                             value=counter)
            print(val)

        objRelAlg.update_schemas(rel + "_tmp", schema)
        objRelAlg.write_to_pages(rel, res)

        return rel

    def proj(self, rel, attList):
        """
            project relation rel on attributes in attList, which is a list of strings, corresponding to a list of
            attributes in relation rel.
            :param rel: rel on attributes in attList
            :param attList: list of strings, corresponding to a list of attributes in relation rel.
            :return: Return the name of the resulting relation. The schema for the resulting relation is the set of
                     attributes in attList.
        """
        tmp_path = self.data_dir + rel + "_tmp"
        path = tmp_path if os.path.exists(tmp_path) else self.data_dir + rel
        # print(path + self.pg_lnk_file)
        with open(path + self.pg_lnk_file) as pl:
            content = pl.read()
            page_files = json.loads(content)

        data = []
        for page_file in page_files:
            with open(path + "/" + page_file) as f:
                content = f.read()
                page_data = json.loads(content)
                data += page_data

        schema = objRelAlg.relation_schema(rel)
        df = pd.DataFrame(data, columns=schema)
        df = df.filter(attList)
        df.drop_duplicates(keep=False, inplace=True)
        data = df.values.tolist()

        res = objRelAlg.proj_rel_name(attList, rel)
        objRelAlg.update_schemas(res, attList)
        objRelAlg.write_to_pages(res, data)

        return res

    def join(self,rel1, att1, rel2, att2):
        """
        join two relations rel1 and rel2 based on join condition rel.att1 = rel2.att2. Returns the name of the
        resulting relation, with schema being the union of the schemas for rel1 and rel2, minus either att1 or att2.
        :param rel1:
        :param att1:
        :param rel2:
        :param att2:
        :return: Returns the name of the resulting relation
        """
        tmp_rel1_path = self.data_dir + rel1 + "_tmp"
        if os.path.exists(tmp_rel1_path):
            rel1 = rel1 + "_tmp"
        tmp_rel2_path = self.data_dir + rel2 + "_tmp"
        if os.path.exists(tmp_rel2_path):
            rel2 = rel2 + "_tmp"
        schema1 = objRelAlg.relation_schema(rel1)
        schema2 = objRelAlg.relation_schema(rel2)
        att1_pos = schema1.index(att1)
        att2_pos = schema2.index(att2)
        schema = schema1 + schema2
        schema.pop(att1_pos)

        data = []
        tree_root = None
        with open(self.index_dir + self.index_file) as id_:
            indices = json.loads(id_.read())
            for index in indices:
                if index[self.rel_pos] == rel2 and index[self.attr_pos] == att2:
                    tree_rel, tree_root = rel2, index[self.root_pos]
                    break

        if tree_root:
            with open(self.data_dir + rel1 + self.pg_lnk_file) as pl1:
                rel1_page_files = json.loads(pl1.read())

            for rel1_page_file in rel1_page_files:
                with open(self.data_dir + rel1 + "/" + rel1_page_file) as pg1:
                    rel1_tuples = json.loads(pg1.read())
                    for rel1_tuple in rel1_tuples:
                        res = objRelAlg.join_by_index(rel2, att2, rel1_tuple[att1_pos])
                        new_data = [rel1_tuple + r for r in res]
                        new_data = [nd[:att1_pos] + nd[att1_pos + 1:] for nd in new_data]
                        data += new_data
        else:
            with open(self.data_dir + rel1 + self.pg_lnk_file) as pl1, open(
                    self.data_dir + rel2 + self.pg_lnk_file) as pl2:
                rel1_page_files = json.loads(pl1.read())
                rel2_page_files = json.loads(pl2.read())

            for rel1_page_file in rel1_page_files:
                with open(os.path.join(self.data_dir, rel1, rel1_page_file)) as pg1:
                    rel1_tuples = json.loads(pg1.read())

                    for rel2_page_file in rel2_page_files:
                        with open(os.path.join(self.data_dir, rel2, rel2_page_file)) as pg2:
                            rel2_tuples = json.loads(pg2.read())
                            new_data = [t1 + t2 for t1 in rel1_tuples for t2 in rel2_tuples if
                                        t1[att1_pos] == t2[att2_pos]]
                            new_data = [nd[:att1_pos] + nd[att1_pos + 1:] for nd in new_data]
                            data += new_data

        res = objRelAlg.join_rel_name(rel1, rel2)
        objRelAlg.update_schemas(res, schema)
        objRelAlg.write_to_pages(res, data)
        return res


if __name__ == '__main__':
    obj = RelAlgebra()

    ## Calling functions
    # obj.sel()
    # obj.proj()
    # obj.join()

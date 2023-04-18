"""
    query.py, contains the implementation for the queries using relation algebra functions. These queries are specified
    in Section 5.
"""

from dbproj.program.display import Display
from dbproj.program.relAlg import RelAlgebra
from dbproj.program.remove import RemoveElements
from dbproj.program.Dynamic_Paths import *
from dbproj.program.buildTree import BuildTree
from dbproj.program.displayHelper import DisplayHelper


relobj = RelAlgebra()
remElobj = RemoveElements()
dispobj = Display()
dispBPTreeobj = DisplayHelper()
bldTreeobj = BuildTree()
SUPPLY = "Supply"
PRODUCTS = "Products"
SUPPLIERS = "Suppliers"

class Queries:
    def __init__(self):
        pass

    def part_a(self):
        order = 2
        bldTreeobj.build('Suppliers', 'sid', order)
        ques1 = "Question 1: Find the name for the supplier 's23' when a B+_tree exists on Suppliers.sid. \r\n"
        with open(OUTPUT_DIR + QUERY_RSLT_FILE, "a+") as qr:
            qr.write(ques1)
        res = self.query_a()
        dispBPTreeobj.display_schema(res, QUERY_RSLT_FILE)
        dispobj.displayTable(res, QUERY_RSLT_FILE)
        remElobj.removeTable(res)
        remElobj.removeTree(SUPPLIERS, "sid")

    def query_a(self):
        tmp_result = relobj.sel(SUPPLIERS, "sid", "=", "s23")
        query_result = relobj.proj(tmp_result, ["sname"])
        remElobj.removeTable("Suppliers_tmp")
        return query_result

    def part_b(self):
        ques2 = "Question 2: Remove the B+_tree from Suppliers.sid, and repeat Question a.\r\n"
        with open(OUTPUT_DIR + QUERY_RSLT_FILE, "a+") as qr:
            qr.write(ques2)
        res = self.query_b()
        dispBPTreeobj.display_schema(res, QUERY_RSLT_FILE)
        dispobj.displayTable(res, QUERY_RSLT_FILE)
        remElobj.removeTable(res)

    def query_b(self):
        tmp_result = relobj.sel(SUPPLIERS, "sid", "=", "s23")
        query_result = relobj.proj(tmp_result, ["sname"])
        remElobj.removeTable("Suppliers_tmp")
        return query_result

    def part_c(self):
        ques3 = "Question 3: Find the address of the suppliers who supplied 'p15'.\r\n"
        with open(OUTPUT_DIR + QUERY_RSLT_FILE, "a+") as qr:
            qr.write(ques3)
        res = self.query_c()
        dispBPTreeobj.display_schema(res, QUERY_RSLT_FILE)
        dispobj.displayTable(res, QUERY_RSLT_FILE)
        remElobj.removeTable(res)

    def query_c(self):
        tmp_result = relobj.sel(SUPPLY, "pid", "=", "p15")
        tmp_result = relobj.join(tmp_result, "sid", SUPPLIERS, "sid")
        query_result = relobj.proj(tmp_result, ["address"])
        remElobj.removeTable("Supply_tmp")
        remElobj.removeTable(tmp_result)
        return query_result

    def part_d(self):
        ques4 = "Question 4: What is the cost of 'p20' supplied by 'Kiddie'?\r\n"
        with open(OUTPUT_DIR + QUERY_RSLT_FILE, "a+") as qr:
            qr.write(ques4)
        res = self.query_d()
        dispBPTreeobj.display_schema(res, QUERY_RSLT_FILE)
        dispobj.displayTable(res, QUERY_RSLT_FILE)
        remElobj.removeTable(res)

    def query_d(self):
        tmp_res1 = relobj.sel(SUPPLIERS, "sname", "=", "Kiddie")
        tmp_res2 = relobj.sel(SUPPLY, "pid", "=", "p20")
        tmp_result = relobj.join(tmp_res1, "sid", tmp_res2, "sid")
        query_result = relobj.proj(tmp_result, ["cost"])
        remElobj.removeTable("Suppliers_tmp")
        remElobj.removeTable("Supply_tmp")
        remElobj.removeTable(tmp_result)
        return query_result

    def part_e(self):
        ques5 = "Question 5: For each supplier who supplied products with a cost of 47 or higher, " \
                "list his/her name, product name and the cost.\r\n\r\n"
        with open(OUTPUT_DIR + QUERY_RSLT_FILE, "a+") as qr:
            qr.write(ques5)
        res = self.query_e()
        dispBPTreeobj.display_schema(res, QUERY_RSLT_FILE)
        dispobj.displayTable(res, QUERY_RSLT_FILE)
        remElobj.removeTable(res)

    def query_e(self):
        tmp_result = relobj.sel(SUPPLY, "cost", ">=", 47.00)
        tmp_res1 = relobj.join(tmp_result, "pid", PRODUCTS, "pid")
        tmp_res2 = relobj.join(tmp_res1, "sid", SUPPLIERS, "sid")
        query_result = relobj.proj(tmp_res2, ["sname", "pname", "cost"])
        remElobj.removeTable("Supply_tmp")
        remElobj.removeTable(tmp_res1)
        remElobj.removeTable(tmp_res2)
        return query_result


if __name__ == "__main__":
    obj = Queries()

    print("For 8-a")
    obj.part_a()

    print("For 8-b")
    obj.part_b()

    print("For 8-c")
    obj.part_c()

    print("For 8-d")
    obj.part_d()

    print("For 8-e")
    obj.part_e()

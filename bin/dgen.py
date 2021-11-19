#!/usr/bin/python

# TODO LIST
# - Create help command
# - Create -h response for each command
# - Refine prints and asserts
# - Create show-config command
# - Create hist command
# - Document each function
# - Optimize block of code labeled with TABLE and BLOCK

import sys
import time
import cx_Oracle
from getpass import getpass
from tqdm import tqdm
import random
import sys
import configparser
import pandas as pd

class DataGen:
    
    def __init__(self, args):
        self.host = None
        self.port = None
        self.user = None
        self.pasw= None
        self.sid = None
        self.connection = None
        self.cursor = None
        
        command = args[1]
        params_values = args[2:]
        assert len(params_values) %2 ==0, "[conn] Fatal error, missing arguments"
        
        params = []
        values = []
        for i, pv in enumerate(params_values):
            if i % 2 == 0:
                params.append(pv)
            else:
                values.append(pv)
        params_values = {k:v for k,v in zip(params,values)}
        
        if command=="set-config":

            assert "-l" in params_values, "Define -l, which is the 'host' or 'DB enpoint' (RDS Adress)."
            assert "-s" in params_values, "Define -s, which is the 'SID' (Oracle System ID), used to identify a database on a system."
            assert "-u" in params_values, "Define -u, which is the username of the database."
            assert "-x" in params_values, "Define -x, which is the password related to the username."
            
            host = params_values["-l"]
            sid  = params_values["-s"]
            user = params_values["-u"]
            pasw = params_values["-x"]
            #port = params_values["-p"]
            #port = int(self.port)
            
            start = time.time()
            _ = cx_Oracle.connect(
                user=user,
                password=pasw,
                dsn=f"{host}/{sid}")
            
            config = configparser.ConfigParser()
            config['DEFAULT'] = params_values
            with open('config.conf', 'w') as configfile:
                config.write(configfile)
            
            dtime = time.time() - start
            print("[conn] Configured. ET: {0:.5f}s".format(dtime))
            
        if command=="init":
            if len(params_values) == 0:
                if self.connect():
                    self.drop_tables()
                    self.create_tables()
                    self.disconnect()
            else:
                print("Error. No parameters required")
                
        elif command=="insert":
            assert "-t" in params_values, "Please define -t (table)"
            assert "-n" in params_values, "Please define -n (rows)"
            assert "-b" in params_values, "Please define -b (blocks)"
           
            table = params_values["-t"]
            rows = params_values["-n"]
            blocks = params_values["-b"]
            delay = params_values["-d"] if "-d" in params_values else 0
            
            if self.connect():
                self.insert(table,rows,blocks,delay)
                self.disconnect()
        
        elif command=="update":
            assert "-t" in params_values, "Please define -t (table)"
            assert "-b" in params_values, "Please define -b (blocks)"
            assert "-m" in params_values, "Please define -m (text message)"
           
            table = params_values["-t"]
            blocks = params_values["-b"]
            text = params_values["-m"]
            
            if self.connect():
                self.update(table,blocks,text)
                self.disconnect()

        elif command=="delete":
            assert "-t" in params_values, "Please define -t (table)"
            assert "-b" in params_values, "Please define -u (blocks)"
           
            table = params_values["-t"]
            blocks = params_values["-b"] 
            
            if self.connect():
                self.delete(table,blocks)
                self.disconnect()
                
        elif command=="data":
            assert "-t" in params_values, "Please define -t (table)"
            assert "-b" in params_values, "Please define -u (blocks)"
           
            table = params_values["-t"]
            blocks = params_values["-b"]
            
            if self.connect():
                self.data(table,blocks)
                self.disconnect()
        
        elif command=="info":
            if len(params_values) == 0:
                if self.connect():
                    self.info()
                    self.disconnect()
            print("[info] No parameters required")
            
        elif command=="drop":
            if len(params_values) == 0:
                if self.connect():
                    self.drop_tables()
                    self.disconnect()
            print("[drop] No parameters required")

        elif command=="show-config":
            # TODO
            pass

        elif command=="hist":
            # TODO
            pass

        elif command=="help":
            # TODO
            pass

        else:
            print("Fatal error, unexpected command.")
    
    def connect(self):
        """
        
        """
        config = configparser.ConfigParser()
        config.read('config.conf')
        try:
            self.host = config['DEFAULT']["-l"]
            self.sid  = config['DEFAULT']["-s"]
            self.user = config['DEFAULT']["-u"]
            self.pasw = config['DEFAULT']["-x"]
            #self.port = config['DEFAULT']["-p"]
            #self.port = int(self.port)
            
            start = time.time()
            self.connection = cx_Oracle.connect(
                user=self.user,
                password=self.pasw,
                dsn=f"{self.host}/{self.sid}")
            self.cursor = self.connection.cursor()
            dtime = time.time() - start
            print(f"[cxnt] Connected. ET: {dtime}s")
            return True
        except Exception as e:
            print(f"[cxnt] Fatal Error: \n{e}") 
            return False
    
    def disconnect(self):
        self.connection.close()
        print(f"[cxnt] Disconected")
    
    def create_tables(self, verbose = False):
        print("[tbls] Creating tables ...")
        start = time.time()
        
        table1 = """CREATE TABLE "TABLE1" (
            "t1_pk" INT NOT NULL, 
            "block" INT NOT NULL, 
            "number" NUMBER(10,2) NOT NULL, 
            "var" VARCHAR2(17) NOT NULL,
            "cdc" VARCHAR2(25),
            CONSTRAINT "TABLE1_CONSTRAINT" PRIMARY KEY ("t1_pk")
        )"""
        table2 = """CREATE TABLE "TABLE2"(
            "t2_pk" INT NOT NULL, 
            "block" INT NOT NULL, 
            "number" NUMBER(10,2) NOT NULL, 
            "var" VARCHAR2(17) NOT NULL,
            "cdc" VARCHAR2(25),
            "refrence_t1" INT,
            CONSTRAINT "TABLE2_CONSTRAINT" FOREIGN KEY ("refrence_t1") REFERENCES TABLE1("t1_pk") ON DELETE CASCADE
        )"""
        table3 = """CREATE TABLE "TABLE3"(
            "block" INT NOT NULL, 
            "number" NUMBER(10,2) NOT NULL, 
            "var" VARCHAR2(17) NOT NULL,
            "cdc" VARCHAR2(25)
        )"""
        plsql_config = """
            begin
                rdsadmin.rdsadmin_util.force_logging(p_enable => true);
                rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD');
                rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD',p_type => 'ALL');
                rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD',p_type => 'PRIMARY KEY');
                rdsadmin.rdsadmin_util.switch_logfile;
                rdsadmin.rdsadmin_master_util.create_archivelog_dir;
                rdsadmin.rdsadmin_master_util.create_onlinelog_dir;
            end;
        """
        
        self.sql(table1, "tables", "TABLE1 created", "TABLE1 warning", verbose)
        self.sql(table2, "tables", "TABLE2 created", "TABLE2 warning", verbose)
        self.sql(table3, "tables", "TABLE3 created", "TABLE3 warning", verbose)
        self.sql(plsql_config, "table", "PL/SQL Executed", "PL/SQL warning", verbose)
        self.connection.commit()
        
        dtime = time.time() - start
        print("[tbls] Tables created. ET: {0:.5f}s".format(dtime))
    
    def drop_tables(self, verbose=False):
        start = time.time()
        self.sql("DROP TABLE TABLE3", "tables", "TABLE3 droped", "TABLE3 warning", verbose)
        self.sql("DROP TABLE TABLE2", "tables", "TABLE2 droped", "TABLE2 warning", verbose)
        self.sql("DROP TABLE TABLE1", "tables", "TABLE1 droped", "TABLE1 warning", verbose)        
        self.connection.commit()
        dtime = time.time() - start
        print("[tbls] Tables dropped. ET: {0:.5f}s".format(dtime))
    
    def insert(self, table, nrows=10, blckr="1,2", delay=0):
        print("[data] INSERT")
        
        # -------------
        # TABLE
        try:
            table = table.replace(" ","").lower()
            assert table in ["table1","table2","table3","all"]
            table = ["table1","table2","table3"] if table == "all" else [table] 
        except:
            print("[data] ERROR - Invalid table, expected values: 'table1','table2','table3' or 'all'")
            return 
        
        # -------------
        # BLOCKS
        try:
            assert len(blckr) > 0
            blckr = list(map(lambda x: int(x),blckr.split(",")))
        except:
            print("[data] ERROR - Invalid range, some expected examples: 2,5 or 1,2,3 or 4,5")
            return
        
        # -------------
        # NEW ROWS
        try: 
            nrows = int(nrows)
            assert 1 <= nrows <= 10000
        except:
            print("[data] ERROR - Invalid number of rows [valid range 1 - 10000].")
            return 
    
        # -------------
        # DELAYS
        try:
            delay = int(delay)
            assert 0 <= delay <= 1000
            delay = delay/1000
        except:
            print("[data] ERROR - Invalid delay [valid range 0 - 1000].")
            return 

        datos_t1 = []
        datos_t2 = []
        datos_t3 = []
        pks = []
        print("[data] Inserting data ...")  
        time.sleep(0.01)  
        start = time.time()
        current_table=0
        try:
            if "table1" in table:
                print("[data] table1 ...")  
                current_table=1
                insert_t1 = list(self.cursor.execute("""SELECT MAX("t1_pk") FROM TABLE1"""))[0][0]
                insert_t1 = 0 if not insert_t1 else insert_t1 + 1
                for i in tqdm(range(nrows)):
                    d = (insert_t1 + i,
                         blckr[random.randint(0,len(blckr)-1)],
                         random.randint(0,1000),
                         str(hex(random.randint(0,1000))[2:]),
                         str(0)
                    )
                    self.cursor.execute(f"""INSERT INTO TABLE1 VALUES {d}""")
                    self.connection.commit()
                    pks.append(str(insert_t1 + i))
                    time.sleep(delay)

            if "table2" in table:
                print("[data] table1 ...") 
                current_table=2
                stored_pks = list(self.cursor.execute("""SELECT "t1_pk" FROM TABLE1"""))                
                stored_pks = list(map(lambda x: x[0], stored_pks))
                pks = list(set(pks + stored_pks))
                assert len(pks) > 0, "No primary keys found in TABLE1"
                
                insert_t2 = list(self.cursor.execute("""SELECT MAX("t2_pk") FROM TABLE2"""))[0][0]
                insert_t2 = 0 if not insert_t2 else insert_t2 + 1
                for i in tqdm(range(nrows)):
                    d = (insert_t2 + i,
                         blckr[random.randint(0,len(blckr)-1)],
                         random.randint(0,1000),
                         str(hex(random.randint(0,1000))[2:]),
                         str(0),
                         pks[random.randint(0,len(pks)-1)]
                    )
                    self.cursor.execute(f"""INSERT INTO TABLE2 VALUES {d}""")
                    self.connection.commit()
                    time.sleep(delay)
 
            if "table3" in table:
                print("[data] table3 ...") 
                current_table=3
                for i in tqdm(range(nrows)):
                    d = (blckr[random.randint(0,len(blckr)-1)],
                         random.randint(0,1000),
                         str(hex(random.randint(0,1000))[2:]),
                         str(0)
                    )
                    self.cursor.execute(f"""INSERT INTO TABLE3 VALUES {d}""")
                    self.connection.commit()
                    time.sleep(delay)
            
        except Exception as e:
            print(f"[data] Fatal error while insertign data in table {current_table}: \n{e}") 
            return
        
        dtime = time.time() - start
        print(f"[data] Inserts completed: elapsed time {dtime}s")
        
    def update(self, table, blckr, cdctx):
        print("[data] UPDATE")
        cdctx = cdctx[:20]
        
        # -------------
        # TABLE
        try:
            table = table.replace(" ","").lower()
            assert table in ["table1","table2","table3","all"]
            table = ["table1","table2","table3"] if table == "all" else [table] 
        except:
            print("[data] ERROR - Invalid table. Expected values: 'table1','table2','table3' or 'all'")
            return 
        
        # -------------
        # BLOCKS
        try:
            where = ""
            blckr = blckr.replace(" ","").lower()
            if blckr!="all":
                blckr = list(map(lambda x: str(int(x)), blckr.split(",")))
                blckr = ",".join(blckr)
                where = f"""WHERE "block" IN ({blckr})"""  
        except:
            print("[data] ERROR - Invalid range, some example of the expected values: 2,5 or 1,2,3 or 5,6")
            return
        
        print("[data] Updating data ...") 
        start = time.time()    
        try:
            for t in table:
                self.cursor.execute(f"""UPDATE {t} SET "cdc"='{cdctx}' {where}""")
                self.connection.commit()
        except Exception as e:
            print(f"[data: ERROR] Fatal error while updating data: \n{e}") 
            return
        dtime = time.time() - start
        print(f"[data] Update completed: elapsed time {dtime}s")
        
    def delete(self, table, blckr):
        print("[data] DELETE")
        time.sleep(0.01)
        
        # -------------
        # TABLE
        try:
            table = table.replace(" ","").lower()
            assert table in ["table1","table2","table3","all"]
            table = ["table1","table2","table3"] if table == "all" else [table] 
        except:
            print("[data: ERROR] Invalid table. Expected values: 'table1','table2','table3' or 'all'")
            return 
        
        # -------------
        # BLOCKS
        try:
            where = ""
            blckr = blckr.replace(" ","").lower()
            if blckr!="all":
                blckr = list(map(lambda x: str(int(x)), blckr.split(",")))
                blckr = ",".join(blckr)
                where = f"""WHERE "block" IN ({blckr})"""  
        except:
            print("[data: ERROR] Invalid range. Expected values: Example: 2,5")
            return

        print("[data] Deleting data ...") 
        time.sleep(0.01)
        start = time.time()
        try:
            for t in table:
                self.cursor.execute(f"""DELETE {t} CNT {where}""")
                self.connection.commit()
        except Exception as e:
            print(f"[data: ERROR] Fatal error while deleting data: \n{e}")
            return
        dtime = time.time() - start
        print(f"[data] Delete completed: elapsed time {dtime}s")
    
    def info(self):
        print("[data] INFO")
        try:
            for table in [1,2,3]:
                count = list(self.cursor.execute(f"""SELECT count(*) FROM TABLE{table}"""))[0][0]
                print("   ",f"TABLE{table} # rows:",count)
                blocks = list(self.cursor.execute(f"""SELECT UNIQUE("block") FROM TABLE{table}"""))
                blocks = list(map(lambda x: str(x[0]),blocks))
                print("           blocks:"," ".join(blocks))
                print()
        except Exception as e:
            print(f"[data: ERROR] Fatal error while fetching data: \n{e}")
            
    def data(self, table, blckr):
        print("[data] SHOW DATA")
        
        # -------------
        # TABLE
        try:
            table = table.replace(" ","").lower()
            assert table in ["table1","table2","table3"]
        except:
            print("[data: ERROR] Invalid table. Expected values: 'table1','table2' or 'table3'")
            return 
        
        # -------------
        # BLOCKS
        try:
            where = ""
            blckr = blckr.replace(" ","").lower()
            if blckr!="all":
                blckr = list(map(lambda x: str(int(x)), blckr.split(",")))
                blckr = ",".join(blckr)
                where = f"""WHERE "block" IN ({blckr})"""  
        except:
            print("[data: ERROR] Invalid range. Expected values: Example: 2,5")
            return
        
        try:
            sql = f"""SELECT * FROM {table} {where} FETCH FIRST 300 ROWS ONLY"""
            data = list(self.cursor.execute(sql))
            
            if table == "table1":
                columns = ["t1_pk", "block", "number", "var" ,"cdc"]
            if table == "table2":
                columns = ["t2_pk", "tag", "number", "var", "cdc", "refrence_t1"]
            if table == "table3":
                columns = ["tag", "number", "var","cdc"]
            df =pd.DataFrame(data, columns=columns )
            if len(df):
                print(df.to_string())
            else:
                print("NO DATA.")
        except Exception as e:
            print(f"[data: ERROR] Fatal error while fetching data: \n{e}")
    
    def _input(self, msn, tabs=0, password=False):
        t = ''.join(["  "]*tabs)
        if password:
            return getpass(f"{t}{msn}:") 
        return input(f"{t}{msn}: ").strip()
    
    def sql(self, sql, command, msn, msnerr, verbose=False):
        try:
            self.cursor.execute(sql)
            print(f"[{command}] {msn}")
        except Exception as e:
            if verbose:
                print(f"[{command}] {msnerr}: \n{e}")

DataGen(sys.argv)


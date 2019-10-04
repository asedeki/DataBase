import sqlite3 as sq
from optparse import OptionParser
import numpy as np
# from numba import jit
def opt():
    parser = OptionParser()
    parser.add_option("-d", "--database", action="store", dest="database", type="string",
                      default=":memory:", help="""le nom de la base de données
                                       """)
    #
    parser.add_option("-f",action="store", dest="data_file", type="string")
    (_val, args) = parser.parse_args()
    return _val


class Quasi1dData():
    database = None
    db_param = {
        "name": "quasi1d.db",
        "table": {
            "PARAMETERS": ["tp float default 200",
                           "tp2 float NOT NULL",
                           "Temperature float NOT NULL",
                           "g1 float default 0.32",
                           "g2 float default 0.64",
                           "g3 float default 0.0",
                           "Np int default 32",
                           "error float default 1e-3",
                           "Ef float default 3000",
                           "time text default NULL"
                           ],
            "INTERACTIONS": ["parametersId int",
                             "g1 float",
                             "g2 float",
                             "g3 float"
                             ]
        }
    }
    parameters = {}
    interactions = {}

    def __new__(cls, *vargs, **kwargs):
        if cls.database is None:
            cls.database = super(Quasi1dData, cls).__new__(cls)
        return cls.database

    def __init__(self, database=None, **kwargs):
        for param in self.db_param["table"]["PARAMETERS"]:
            param = param.split()[0]
            self.parameters[param] = None
        for k in self.db_param["table"]["INTERACTIONS"]:
            k = k.split()[0]
            self.interactions[k] = None

        self.parameters.update(kwargs)
        self.connect(database)
        self.cursor.execute("PRAGMA foreign_keys = ON")

    def connect(self, database):
        if database is None:
            database = self.db_param["name"]
        self.connection = sq.connect(database)
        self.cursor = self.connection.cursor()

    def create_table(self, table="PARAMETERS"):
        keys = ", ".join(
            self.db_param["table"][table]
        )
        request = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {table.lower()}Id
            INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            {keys}
            )
        """
        # # input(request)
        self.cursor.execute(request)
        self.connection.commit()

    def save_interactions(self, **parameters):
        assert(
            set(parameters).issubset(
                set(self.interactions.keys())
            )
        )
        params = list(parameters.keys())
        val = ["?"] * len(params)
        values = tuple(parameters[k] for k in params)
        request = f"""
            INSERT INTO INTERACTIONS
            ({",".join(params)})
            VALUES ({",".join(val)})
        """
        self.cursor.execute(request, values)


    def save_parameters(self, **parameters):
        assert(
            set(parameters).issubset(
                set(self.parameters.keys())
            )
        )
        done = True
        parameters_id = self.get_parameters_id(**parameters)
        # input(parameters_id)
        if parameters_id != -1:
            return not done, parameters_id[0]
        else:
            params = list(parameters.keys())
            val = ["?"] * len(params)
            values = tuple(parameters[k] for k in params)
            request = f"""
                INSERT INTO PARAMETERS
                ({",".join(params)})
                VALUES ({",".join(val)})
            """
            self.cursor.execute(request, values)
            self.connection.commit()
            return done, self.get_parameters_id(**parameters)[0]
        return (False, 1)

    def get_parameters_id(self, **parameters):

        condition = ""
        if parameters == {}:
            condition = f"TRUE    "
        else:
            for param, value in parameters.items():
                if value is not None: # and param != "time":
                    condition += f"{param}={value} and "
        request = f"""
            SELECT parametersId FROM PARAMETERS WHERE
            {condition[:-4]}
        """
        response = self.cursor.execute(request)
        parameters_id = response.fetchall()
        if len(parameters_id) == 0:
            return -1
        else:
            parameters_id = [i[0] for i in parameters_id]
            return parameters_id

    def get_parameters(self, *selected_names, table="PARAMETERS", **parameters):
        if parameters == {}:
            condition = "TRUE"
        else:
            condition = ""
            for param, value in parameters.items():
                condition += f"{param}={value} and "
            condition = condition[:-4]
        if selected_names == ():
            selected_names = [k[:k.index(" ")]
                              for k in self.db_param["table"][table]
                              ]
        request = f"""
            SELECT {",".join(selected_names)}
            FROM {table} WHERE
            {condition}
            order by {",".join(selected_names)}
        """
        print(request)
        result = self.cursor.execute(request)
        return selected_names, result.fetchall()

    def drop_db(self, **kwargs):
        for table in self.db_param["table"]:
            self.cursor.execute(f"""
                                DROP TABLE IF EXISTS
                                {table}
                                """)
        self.connection.commit()


def update_parameters(db, file):
    parameters={}
    idT={}
    data = np.load(file,allow_pickle=True)[()]
    parameters.update(data["param"])
    del data["param"]
    if "time" in data:
        timep = data["time"]
        del data["time"]
    else:
        timep = None
    for T in data.keys():
        parameters["Temperature"]= T
        try:
            parameters["time"] = data[T]["time"]
            del data[T]["time"]
        except:
            parameters["time"] = timep
        if not isinstance(data[T], list):
            data[T] = data[T]["g"]
        # input(type(data[T]))

        idT[T] = [db.save_parameters(**parameters), parameters["Np"]]
    return idT, data

# @jit
def update_interaction(db, idT, data):
    Np = data
    values={}
    for T in idT.keys():
        if idT[T][0][0]:
            values["parametersId"] = idT[T][0][1]
            Np = int(idT[T][1])
            # input(data[T])
            for i in range(Np):
                for j in range(Np):
                    for k in range(Np):
                        values["g1"] = data[T][0][i,j,k]
                        values["g2"] = data[T][1][i,j,k]
                        values["g3"] = data[T][2][i,j,k]
                        db.save_interactions(**values)
            db.connection.commit()
def main(db, _O):
    from glob import glob
    import os
    import time
    import math
    tini = time.time()
    if os.path.exists(_O.data_file):
        if os.path.isfile(_O.data_file):
            id_param_T, data = update_parameters(db, _O.data_file)
    else:
        files = glob("*.npy")
        # input(files)
        for f in files:
            id_param_T, data = update_parameters(db, f)
    update_interaction(db, id_param_T, data)
    print(f"temps_exec = {time.time()-tini}")

if __name__ == "__main__":
    _O = opt()
    db = Quasi1dData(database=_O.database)

    # db.drop_db()
    # db.create_table("PARAMETERS")
    # db.create_table("INTERACTIONS")
    # main(db, _O)
    # main(db, _O)

    selected_names, result = db.get_parameters("tp2","Temperature",table="PARAMETERS")

    # g1 = np.zeros((32,32,32), float)
    # ir=0
    # for i in range(32):
    #     for j in range(32):
    #         for k in range(32):
    #             g1[i,j,k] = result[ir][0]
    #             ir += 1
    # print(np.all(g1 - data[100][1]==0.0))


    str_result = ""
    for r in result:
        for u, v in zip(selected_names,r):
            str_result += f"{u}={v} ,"
        str_result += "\n ========================\n"
    print(str_result)

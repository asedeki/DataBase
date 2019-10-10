# -*- coding: utf_8 -*-
import sqlite3 as sq
from optparse import OptionParser
import numpy as np
import concurrent.futures as ccf

# from numba import jit


def opt():
    parser = OptionParser()
    parser.add_option("-d", "--database", action="store", dest="database", type="string",
                      default=":memory:", help="""le nom de la base de données
                                       """)
    #
    parser.add_option("-f", action="store", dest="data_file", type="string")
    parser.add_option("-r", action="store", dest="remarque", type="string", default="",
                      help="le champs remarque de la BD PARAMETERS")

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
                           "rel_tol float default 1e-3",
                           "Ef float default 3000",
                           "time text default NULL",
                           "remarque text default NULL"
                           ],
            "INTERACTION": ["interactionId INTEGER NOT NULL",
                            "parametersId INTEGER NOT NULL",
                            "g1 float",
                            "g2 float",
                            "g3 float"
                            ],
            "SUSCEPTIBILITY": [
                "parametersId INTEGER NOT NULL",
                "CSDW_0 float", "CSDW_pi float",
                "CBDW_0 float", "CBDW_pi float",
                "SSDW_0 float", "SSDW_pi float",
                "SBDW_0 float", "SBDW_pi float",
                "SS_s float", "SS_dxy float", "SS_dx2y2 float", "SS_g float", "SS_i float",
                "ST_px float", "ST_py float", "ST_h float", "ST_f float"
            ]
        }
    }
    parameters = {}
    interactions = {}
    susceptibilities = {}

    def __new__(cls, *vargs, **kwargs):
        # if cls.database is None:
        cls.database = super(Quasi1dData, cls).__new__(cls)
        return cls.database

    def __init__(self, database, **kwargs):
        for param in self.db_param["table"]["PARAMETERS"]:
            param = param.split()
            ik = param.index('default') if 'default' in param else None
            self.parameters[param[0]] = param[ik +
                                              1] if ik is not None else None
        for param in self.db_param["table"]["INTERACTION"]:
            param = param.split()
            ik = param.index('default') if 'default' in param else None
            self.interactions[param[0]] = param[ik +
                                                1] if ik is not None else None
        for param in self.db_param["table"]["SUSCEPTIBILITY"]:
            param = param.split()
            ik = param.index('default') if 'default' in param else None
            self.susceptibilities[param[0]] = param[ik +
                                                    1] if ik is not None else None

        self.request = {t: {} for t in self.db_param["table"].keys()}
        self.db_param['name'] = database
        self.parameters.update(kwargs)
        self.connect()

    def connect(self):
        database = self.db_param["name"]
        self.connection = sq.connect(database)
        self.cursor = self.connection.cursor()
        # self.cursor.execute("PRAGMA foreign_keys = ON")

    def create_table(self, table="PARAMETERS"):

        if table == "PARAMETERS":
            keys = f"""
                        {table.lower()}Id
                    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                    {", ".join(
                        self.db_param["table"][table]
                    )}
                    """

        elif table == "INTERACTION":
            keys = f"""
                {", ".join(
                        self.db_param["table"][table]
                )},
                PRIMARY KEY({table.lower()}Id,parametersId),
                FOREIGN KEY (parametersId) REFERENCES PARAMETERS(parametersId) 
                ON DELETE CASCADE ON UPDATE NO ACTION
            """
        else:
            keys = f"""
                {", ".join(
                        self.db_param["table"][table]
                )},
                PRIMARY KEY(parametersId),
                FOREIGN KEY (parametersId) REFERENCES PARAMETERS(parametersId) 
                ON DELETE CASCADE ON UPDATE NO ACTION
            """
        request = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {keys}
            );
        """
        # input(request)
        self.cursor.execute(request)
        self.connection.commit()

    def create_all(self):
        for table in self.db_param['table'].keys():
            self.create_table(table=table)

    def save_data(self, table, data):
        rows = {r.split()[0] for r in self.db_param['table'][table]}
        # input(rows)
        # input(set(data))
        # input(data.values())
        assert(
            set(data).issubset(rows)
        )
        val = ["?"] * len(rows)
        values = tuple(data[k] for k in rows)
        request = f"""
            INSERT INTO {table}
            ({",".join(rows)})
            VALUES ({",".join(val)})
        """
        self.request[table].setdefault(request, [])
        self.request[table][request].append(values)
        self.cursor.execute(request, values)

    def save_parameters(self, parameters, remarque='scipy.integrate.solve_ivp'):
        assert(
            set(parameters).issubset(
                set(self.parameters.keys())
            )
        )

        done = True
        parameters['remarque'] = remarque
        parameters_id = self.get_parameters_id(**parameters)
        # input(f"idp = {parameters_id}")
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
            #self.request["PARAMETERS"].setdefault(request, [])
            # print(self.request["PARAMETERS"])
            # self.request["PARAMETERS"][request].append(values)
            # input(request)
            self.cursor.execute(request, values)
            self.connection.commit()
            return done, self.get_parameters_id(**parameters)[0]

    def get_parameters_id(self, **parameters):

        condition = ""
        if parameters == {}:
            condition = f"TRUE    "
        else:
            for param, value in parameters.items():
                if value is not None and param != 'remarque':  # and param != "time":
                    condition += f"{param}={value} and "
            condition += f'remarque="{parameters["remarque"]}"'
        request = f"""
            SELECT parametersId FROM PARAMETERS WHERE
            {condition}
        """
        # input(request)
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
        result = self.cursor.execute(request)
        return selected_names, result.fetchall()

    def get_data(self, table, * selected_names, **parameters):
        print(selected_names)
        if len(selected_names) != 0:
            row = ",".join(selected_names)
        else:
            row = "* "
        if parameters == {}:
            condition = ""
        else:
            condition = "WHERE  "
            for param, value in parameters.items():
                condition += f"{param} {value[0]} {value[1]} and "
            condition = condition[:-4]
        if selected_names == ():
            selected_names = [k[:k.index(" ")]
                              for k in self.db_param["table"][table]
                              ]
        request = f"""
            SELECT {row}
            FROM {table}
            {condition}
        """
        # order by {",".join(selected_names)}
        input(request)
        result = self.cursor.execute(request)
        return selected_names, result.fetchall()

    def drop_db(self, **kwargs):
        for table in self.db_param["table"]:
            self.cursor.execute(f"""
                                DROP TABLE IF EXISTS
                                {table}
                                """)
        self.connection.commit()


def update_parameters(db, file, remarque):
    interaction = {}
    susceptibilities = {}
    parameters = {}
    idT = {}
    data = np.load(file, allow_pickle=True)[()]
    parameters.update(data["param"])
    del data["param"]

    parameters['rel_tol'] = data['rel_tol']
    del data['rel_tol']

    if "time" in data:
        timep = data["time"]
        del data["time"]
    else:
        timep = None
    # # input(data.keys())
    for T in data.keys():
        parameters["Temperature"] = T
        try:
            parameters["time"] = data[T]["time"]
            del data[T]["time"]
        except:
            parameters["time"] = timep

        interaction[T] = data[T]["interaction"]
        del data[T]["interaction"]
        susceptibilities[T] = data[T]
        idT[T] = [db.save_parameters(
            parameters, remarque=remarque), parameters["Np"]]
    return idT, interaction, susceptibilities

# @jit


def update_interaction(db, idT, data):
    all_values = []
    values = {}
    for T in idT.keys():
        if idT[T][0][0]:
            values["parametersId"] = idT[T][0][1]
            Np = int(idT[T][1])
            print(T)
            int_id = 0
            for i in range(Np):
                for j in range(Np):
                    for k in range(Np):
                        values["interactionId"] = int_id
                        values["g1"] = data[T]["g1"][i, j, k]
                        values["g2"] = data[T]["g2"][i, j, k]
                        values["g3"] = data[T]["g3"][i, j, k]
                        int_id += 1
                        # # input(values['g1'])
                        # db.save_data('INTERACTION', values)
                        all_values.append(values.copy())

    update = [db.save_data('INTERACTION', v) for v in all_values]
    db.connection.commit()


def update_susceptibility(db, idT, data):
    all_values = []
    values = {}
    for T in idT.keys():
        if idT[T][0][0]:
            values["parametersId"] = idT[T][0][1]
            values.update(data[T]['susc'])
            all_values.append(values.copy())
    update = [db.save_data('SUSCEPTIBILITY', v) for v in all_values]
    db.connection.commit()


def main(db, _O):
    from glob import glob
    import os
    import time
    import math
    tini = time.time()
    if os.path.exists(_O.data_file):
        if os.path.isfile(_O.data_file):
            id_param_T, interaction, susceptibilities = update_parameters(
                db, _O.data_file, _O.remarque
            )
            update_susceptibility(db, id_param_T, susceptibilities)
            update_interaction(db, id_param_T, interaction)
    else:
        files = glob("*.npy")
        # # input(files)
        for f in files:
            id_param_T, interaction, susceptibilities = update_parameters(
                db, f, _O.remarque)
            update_susceptibility(db, id_param_T, susceptibilities)
            update_interaction(db, id_param_T, interaction)
    print(f"temps exec = {time.time()-tini}")
    return interaction, id_param_T

    print(f"temps_exec = {time.time()-tini}")


if __name__ == "__main__":
    import random
    _O = opt()
    db = Quasi1dData(database=_O.database)
    db.create_all()
    data, idT = main(db, _O)
    # selected_names, result = db.get_data(table="PARAMETERS")
    # print(result)

    # TT = random.choice(list(idT.keys()))
    # print(TT)
    # condition = {
    #     "parametersId": ['=', idT[TT][0][1]]
    # }
    # selected_names, result = db.get_data(
    #      "interactionId", "g1", table="INTERACTION", **condition)

    # g_t = {i: g for i, g in result}
    # g1 = np.zeros((32, 32, 32), float)
    # ir = min(g_t)
    # for i in range(32):
    #     for j in range(32):
    #         for k in range(32):
    #             g1[i, j, k] = g_t[ir]

    #print(np.all(g1 - data[TT]['g1'] == 0.0))

    # str_result = ""
    # for r in result:
    #     for u, v in zip(selected_names,r):
    #         str_result += f"{u}={v} ,"
    #     str_result += "\n ========================\n"
    # print(str_result)

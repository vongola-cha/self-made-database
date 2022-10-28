import os
import sys
import pickle
import pandas as pd
import shutil

import csv
import json
import pandas

from G2DB.parser import startParse
from G2DB.core.database import Database
from G2DB.core.table import Table


# All attributes, table names, and database names will be stored in lower case

# database engine
class Engine:

    # action functions
    # CREATE DATABASE test;
    def createDatabase(self, name):
        db = Database(name)
        print('[SUCCESS]: Create Database %s successfully. You also need to save it.' % name)
        return db

    # save database test;
    def saveDatabase(self, db):
        db.save()

    # DROP DATABASE test;
    def dropDatabase(self, db):
        db.drop_database()

    # use database test;
    def useDatabase(self, dbname):
        sysPath = sys.argv[0]
        sysPath = sysPath[:-11] + 'database/'

        if not os.path.exists(sysPath + dbname):
            raise Exception('[ERROR]: No Database called %s.' % dbname)

        elif os.path.exists(sysPath + dbname):
            file = open(sysPath + dbname, "rb")
            db = pickle.load(file)
            file.close()
            print('[SUCCESS]: Open Database %s successfully!' % dbname)

        else:
            raise Exception('[ERROR]: Invalid command.')
        return db

    # show databases;
    def show_database(self):
        t = sys.argv[0]
        t = t[:-11] + 'database/'
        dirs = os.listdir(t)
        for dir in dirs:
            if '.' not in dir:
                print(dir)

    # show tables;
    def show_table(self, db):
        db.display()

    """
    CREATE TABLE table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1);
    CREATE TABLE table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    CREATE TABLE table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    """

    def createTable(self, db, attrs, info):
        db.add_table(attrs, info)
        print('[SUCCESS]: Create Table %s successfully!' % info['name'])
        return db

    # DROP TABLE a;
    def dropTable(self, db, table_name):
        db.drop_table(table_name)

    # insert into perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')
    def insertTable(self, db, table_name, attrs, data):
        db.tables[table_name].insert(attrs, data)
        #print(db.tables[table_name].datalist)
        return db

    def selectQuery(self, db, attrs, tables, where):

        # Return restable
        """
        ats = list(attrs.keys())
        table = db.tables[tables[0]]
        if where:
            cond = {'tag': False, 'sym': where[0]['operation'], 'condition': [where[0]['attr'],  where[0]['value']]}
        else:
            cond = {}
        restable = self.subselect(table, ats, cond)
        return restable
        """
        table_col_dic = {}
        attrs = list(attrs.keys())
        # print(where)
        if attrs != ["*"]:
            # table_col_dic----> {'tableName': ['colName','colName'...]}
            for table_name in tables:
                colNameList = []
                for attr in attrs:
                    if attr in db.tables[table_name].attrls:
                        colNameList.append(attr)

                table_col_dic[table_name] = colNameList

        else:
            # table_col_dic----> {'tableName': ['*']}
            for table_name in tables:
                table_col_dic[table_name] = ['*']

        tc = []
        if tc:
            to = {}
            for tname in table_col_dic.keys():
                to[tname] = self.subselect(db.tables[tname], table_col_dic[tname], [])

            if tc[2]:
                jointable = self.join(to[tc[0]], to[tc[1]], [tc[2]['attr'], tc[2]['value']])
            else:
                jointable = self.join(to[tc[0]], to[tc[1]], [])
            tc.pop(0)
            tc.pop(0)
            tc.pop(0)
            while tc:
                jointable = self.join(jointable, to[tc[0]], [tc[1]['attr'], tc[1]['value']])
                tc.pop(0)
                tc.pop(0)

            info = {'name': 'test', 'attrs': [], 'primary': '', 'foreign': []}
            table = Table(jointable.columns, info)
            table.df = jointable
            table.flag = 1
        ############
        # condition only has number
        ############
        elif len(tables) == 1:
            table = db.tables[tables[0]]

        # print(table)
        # print(attrs)
        # exit()
        ############
        # condition
        ############
        # print(where)
        # exit()
        # TODO add Priority order of and and or
        where_len = len(where)
        if where_len == 1:
            cond = {'tag': where[0]['tag'], 'sym': where[0]['operation'],
                    'condition': [where[0]['attr'], where[0]['value']]}
            restable = self.subselect(table, attrs, cond)
        elif where_len == 0:
            cond = {}
            restable = self.subselect(table, attrs, cond)
        else:
            # has and/ or in where
            dataframe_list = []  # a list
            # push the first dataframe
            cond = {'tag': where[0]['tag'], 'sym': where[0]['operation'],
                    'condition': [where[0]['attr'], where[0]['value']]}
            df = self.subselect(table, attrs, cond)
            dataframe_list.append(df)
            # select
            if attrs[0] == '*':
                attList = list(df.columns)
            else:
                attList = attrs
            # go ahead
            len_where = len(where)
            i = 1
            while i < len_where:
                if where[i] not in ['OR', 'AND', '(', ')']:
                    # push into dataframe_list
                    condtemp = {'tag': where[i]['tag'], 'sym': where[i]['operation'],
                                'condition': [where[i]['attr'], where[i]['value']]}
                    dataframe_list.append(self.subselect(table, attrs, condtemp))

                elif where[i] == 'AND':
                    # inner join
                    df1 = dataframe_list.pop()
                    df2 = dataframe_list.pop()
                    newdf = pd.merge(df1, df2, on=attList, how='inner')
                    if not newdf.empty:
                        dataframe_list.append(newdf)
                i += 1

            # no AND in dataframe_list, outer join all dfs in dataframe_list
            while len(dataframe_list) > 1:
                # outer join
                df1 = dataframe_list.pop()
                df2 = dataframe_list.pop()
                newdf = pd.merge(df1, df2, on=attList, how='outer',sort=False)
                dataframe_list.append(newdf)
            # TODO
            if len(dataframe_list) == 0:
                # print("Empty result! Find no data. Please check the 'WHERE' condition. ")
                restable = None
            else:
                # only has one dataframe left at this time
                restable = dataframe_list.pop()  # only has one dataframe left at this time



        return restable

    ##########################################
    # where:
    # 'tag':        where[i]['tag'],
    # 'sym':        where[i]['operation'],
    # 'condition': [  where[i]['attr'], where[i]['value']  ]
    ##########################################
    def subselect(self, table, attrs, where):
        sym = ''
        tag = False # is not str
        gb = False  # groupby
        condition = []
        if where:
            sym = where['sym']
            tag = where['tag']
            condition = where['condition']
            df = table.search(attrs, sym, tag, condition, gb)
        else:
            df = table.search(attrs, sym, tag, condition, gb)
        return df

    def join(self, table1, table2, attrs):
        df = Database('jointempdb').join_table(table1, table2, attrs)
        return df

    def addor(self, table1, table2, ao):
        if ao == "0":
            df = Database('jointempdb').df_(table1, table2, ao)
        return df

    # TODO
    def check_delete(self, db, table, where, oritablename):
        col = where[0]['attr']
        # print(db.tables[table].data)
        foreignk = table.foreign
        if foreignk=={}:
            return
        ref_info = list(foreignk.values())
        ref_info=ref_info[0]
        # print(ref_info[0])
        # ref_info=[{'schema': ref_table, 'columns': ref_columns, 'on_delete': on_delete.upper(), 'on_update': on_update.upper()}]
        if oritablename == ref_info['schema'][0]:
            refc = ref_info['columns'][0]
            refc=refc[0]
            if refc == col:
                nwhere = []
                nwhere.append({'attr': table.primary[0], 'operation': where[0]['operation'], 'value': where[0]['value']})
                table.delete(table.name, nwhere)

    def delete(self, db, name, where):
        db.tables[name].delete(name, where)
        # TODO
        tables = db.tables
        for table in tables.keys():
            # print(table)
            self.check_delete(db, tables[table], where, name)
        return db

    def update(self, db, name, where, set):
        db = self.delete(db, name, where)
        attrlist = []
        valuelist = []
        for eachset in set:
            attrlist.append(eachset['attr'])
            valuelist.append(eachset['value'])
        db = self.insertTable(db, name, attrlist, valuelist)
        return db

    def createIndex(self, db, table, iname, attr):
        db = db.tables[table].add_index(attr, iname,db,table)
        return db

    def dropIndex(self, db, table, iname):
        db = db.tables[table].drop_index(iname,table)
        return db

    # lauch function: receieve a command and send to execution function.
    def start(self):
        db = None
        # continue running until recieve the exit command.
        while True:
            inputstr = 'GroupTwo>'
            if db:
                inputstr = db.name + '> '
            else:
                inputstr = 'GroupTwo> '
            commandline = input(inputstr)
            ########################
            # Load Test Data
            ########################
            if commandline == 'load demo data':
                demoQuery = 'create database demo;\n'
                testNum = 0
                while testNum < 5:
                    # define data range
                    dataRange = 1000
                    if testNum == 2:
                        dataRange = 10000
                    elif testNum == 4:
                        dataRange = 100000

                    # Rel-i-i-dataRange
                    tableName = 'test' + str(testNum)
                    col1name = 'a'
                    col2name = 'b'
                    demoQuery += 'create table ' + tableName + ' (' + col1name + ' int not_null unique, ' + col2name + ' int unique) primary key (' + col1name + ');\n'
                    for i in range(dataRange + 1):
                        demoQuery += 'insert into ' + tableName + ' (' + col1name + ', ' + col2name + ') values (' + str(
                            i) + ', ' + str(i) + ');\n'
                    testNum += 1

                    # Rel-i-1-dataRange
                    tableName = 'test' + str(testNum)
                    demoQuery += 'create table ' + tableName + ' (' + col1name + ' int not_null unique, ' + col2name + ' int) primary key (' + col1name + ');\n'
                    for i in range(dataRange + 1):
                        demoQuery += 'insert into ' + tableName + ' (' + col1name + ', ' + col2name + ') values (' + str(i) + ', 1);\n'
                    testNum += 1
                demoQuery += 'save database demo;\n'
                # create query over
                # run demo query
                commandlines = demoQuery.split(';\n')
                for commandline in commandlines:
                    # ignore enter and ;
                    if commandline == '':
                        continue
                    commandline = commandline.replace(';', '')
                    # run query
                    try:
                        result, db = self.execute(commandline, db)
                        if result == 'exit':
                            print('[SUCCESS]: Exit successfully! See you next time!')
                            sys.exit(0)
                            return
                    # print error
                    except Exception as err:
                        print(err)
                continue

            ########################
            # Run input query
            ########################
            # ignore enter and ;
            if commandline == '':
                continue
            commandline = commandline.replace(';', '')
            # run query
            try:
                result, db = self.execute(commandline, db)
                if result == 'exit':
                    print('[SUCCESS]: Exit successfully! See you next time!')
                    sys.exit(0)
                    return
            except Exception as err:
                print(err)

    # TODO JOIN
    def joinQuery(self, db, joinType, attrs, tables, joinTableNames, join_condition, where):
        ###############################
        # delect target tablename in where condition
        ###############################
        table_where = ''  # where_target
        if where:
            where_cnt = len(where)
            # print(where)
            i = 0
            while i < where_cnt:
                each_conditon = where[i]
                # print(each_conditon)
                if each_conditon not in ['OR', 'AND', '(', ')']:
                    if '.' in each_conditon['attr']:
                        table_where = each_conditon['attr'].split('.')[0]
                        value = each_conditon['attr'].split('.')[1]
                        each_conditon['attr'] = value
                    else:
                        raise Exception('[ERROR]: Need a specify table in where condition!')
                try:
                    a = int(each_conditon['attr'])
                    a = int(each_conditon['value'])
                    each_conditon['tag'] = 0
                except:
                    pass
                i = i + 1

        ###############################
        # start join
        ###############################
        # selectQuery(db, action['attrs'], action['tables'], action['where'])
        howType = ''
        if joinType == 0:
            # inner join
            howType = 'inner'  # CAN NOT CHANGE!
        elif joinType == 1:
            howType = 'outer'
        elif joinType == 2:
            howType = 'left'
        elif joinType == 3:
            howType = 'right'

        for item in join_condition:
            value1 = item['attr']
            value2 = item['value']
            # get table information
            table1 = value1.split('.')[0]
            col1 = value1.split('.')[1]
            table2 = value2.split('.')[0]
            col2 = value2.split('.')[1]

            # select * from X where [where]
            colName = ['*']
            if table_where == tables[0]:
                df1 = self.selectQuery(db, {'*': 'NORMAL'}, tables, where)
                df2 = self.subselect(db.tables[table2], colName, [])
            elif table_where == table2:
                df2 = self.selectQuery(db, {'*': 'NORMAL'}, [table2], where)
                df1 = self.subselect(db.tables[table1], colName, [])
            elif table_where == '':
                df1 = self.subselect(db.tables[table1], colName, [])
                df2 = self.subselect(db.tables[table2], colName, [])

            # attList = list(attrs.keys())

            # change columns name
            for name in df1.columns:
                # condition key colname won't change
                if name == col1:
                    continue
                new_name = table1 + '.' + name
                df1.rename(columns={name: new_name}, inplace=True)
            for name in df2.columns:
                if name == col2:
                    new_name = col1
                else:
                    new_name = table2 + '.' + name
                df2.rename(columns={name: new_name}, inplace=True)


            # select
            if list(attrs.keys())[0] == '*':
                attList = list(df1.columns)
                attList.extend(list(df2.columns))
                attList = list(set(attList))
            else:
                attList=list(attrs.keys())
            print(attList)

            df = pd.merge(df1, df2, on=col1, how=howType)
            # df = df[att_collist]
            df = df[attList]

        return df

    # execution function: send commandline to parser and get an action as return and execute the mached action function.
    def execute(self, commandline, database):
        # parse the query
        db = database
        action = startParse(commandline)

        if action['query_keyword'] == 'exit':
            return 'exit', db
        ########################
        # CREATE
        ########################
        if action['query_keyword'] == 'create':
            if action['type'] == 'database':
                db = self.createDatabase(action['name'])
            elif action['type'] == 'table':
                if db:
                    if action['name'] in db.tables.keys(): raise Exception(
                        '[ERROR]: Table %s is exsited.' % action['name'])
                    db = self.createTable(db, action['attrls'], action['info'])
                else:
                    raise Exception('[ERROR]: No database name in command/ Cannot find database name.')
            elif action['type'] == 'index':
                if db:
                    db = self.createIndex(db, action['table'], action['index_name'], action['attrs'])
                else:
                    raise Exception('[ERROR]: No database name in command/ Cannot find database name.')
            self.saveDatabase(db)
            return 'continue', db
        ########################
        # DROP
        ########################
        if action['query_keyword'] == 'drop':
            if action['type'] == 'database':
                if db:
                    if action['name'] == db.name:
                        self.dropDatabase(db)
                        db = None
                    else:
                        temp = db
                        db = Database(action['name'])
                        self.dropDatabase(db)
                        db = temp
                else:
                    db = Database(action['name'])
                    self.dropDatabase(db)
                    db = None
            elif action['type'] == 'table':
                self.dropTable(db, action['table_name'])
            elif action['type'] == 'index':
                if db:
                    db = self.dropIndex(db, action['table'], action['index_name'])
                else:
                    raise Exception('[ERROR]: No database name in command/ Cannot find database name.')
            self.saveDatabase(db)
            return 'continue', db
        ########################
        # INSERT
        ########################
        if action['query_keyword'] == 'insert':
            db = self.insertTable(db, action['table_name'], action['attrs'], action['data'])
            self.saveDatabase(db)
            return 'continue', db
        ########################
        # SELECT
        ########################
        if action['query_keyword'] == 'select':
            if db:
                # TODO
                if action['joinType'] != -1:
                    restable = self.joinQuery(db, action['joinType'], action['attrs'], action['tables'],
                                              action['joinTableNames'], action['join_condition'], action['where'])
                else:
                    # no join
                    restable = self.selectQuery(db, action['attrs'], action['tables'], action['where'])
                # TODO add if is None
                if not restable.empty:
                    print(restable)
                else:
                    print("Empty result! Find no data. Please check the 'WHERE' condition. ")
                return 'continue', db
            else:
                raise Exception('[ERROR]: No database name in command/ Cannot find database name.')

        ########################
        # DELETE
        ########################
        if action['query_keyword'] == 'delete':
            db = self.delete(db, action['table'], action['where'])
            self.saveDatabase(db)
            return 'continue', db

        ########################
        # UPDATE
        ########################
        if action['query_keyword'] == 'update':
            db = self.update(db, action['table'], action['where'], action['set'])
            self.saveDatabase(db)
            return 'continue', db

        # ########################
        # # SAVE
        # ########################
        # if action['query_keyword'] == 'save':
        #     if db:
        #         if action['name'] == db.name:
        #             self.saveDatabase(db)
        #         else:
        #             raise Exception('[ERROR]: No database called %s.' % db.name)
        #     else:
        #         raise Exception('[ERROR]: No database called %s.' % db.name)
        #     return 'continue', db

        ########################
        # USE
        ########################
        if action['query_keyword'] == 'use':
            db = self.useDatabase(action['name'])
            return 'continue', db

        ########################
        # SHOW
        ########################
        if action['query_keyword'] == 'show':
            if action['type'] == 'database':
                self.show_database()
            else:
                self.show_table(db)
            return 'continue', db

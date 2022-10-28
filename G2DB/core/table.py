import pandas as pd
import json
import os
import numpy as np
from G2DB.core.hash import HashTable
from G2DB.core.attribute import Attribute
from BTrees.OOBTree import OOBTree

class Table:
    # info = {}
    # need a load() outside
    def __init__(self, attrls, info):
        self.data = {}
        self.datalist = []
        self.df = pd.DataFrame()
        self.name = info['name']
        self.attrls = attrls
        self.attrs = {} #{name: attributeobj}
        self.primary = info['primary']
        self.foreign = info['foreign']
        self.uniqueattr = {} # {attribute_name: {attibute_value: primarykey_value}}
        self.index={}   #{attr: idex_name}
        self.BTree={}   #{idex_name: BTree}
        self.flag = 0

        for attr in info['attrs']:
            temp = Attribute(attr)
            self.attrs[attr['name']] = temp
            if temp.unique:
                self.uniqueattr[attr['name']] = {}

        # TODO: moxiao add_index

    def add_index(self, attr, index_name, db, table):
        # no index on this attr before
        if not os.path.exists('index_info.npy'):
            info = []
            infoarr = np.array(info)
            np.save('index_info.npy', infoarr)
        else:
            # if index_info.npy existed, open it
            infoarr = np.load('index_info.npy', allow_pickle=True)
            info = infoarr.tolist()
            # check if the index was existed, index is the third attribute
            for item in iter(infoarr):
                if (item[2] == index_name): raise Exception(
                    '[ERROR]: Index  is existed.')

        info.append([table, attr, index_name])
        infoarr = np.array(info)
        print(info)
        np.save('index_info.npy', info)
        # create index
        hashlist = [{}, {}, {}, {}, {}, {}, {}]
        df = db.tables[table].search(['*'], [], [], [], False)
        length = df.shape[0]
        for i in range(length):
            row = df.iloc[i]
            value = int(row[attr])
            # print(value)
            hashtable = HashTable(7)
            hash_index = hashtable.hash(value)
            # print(hash_index)
            hashlist[hash_index][value] = row
        # transfer list to array, save as .npy file
        alist = np.array(hashlist)
        # print(dict0.keys())
        # save index as npy.file
        if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
            raise Exception(
                '[ERROR]: Index  is existed.')
        else:
            np.save(table + '_' + ''.join(attr) + '_' + index_name + '.npy', alist)
        # load index
        # a = np.load('a_index.npy', allow_pickle=True)
        # a = a.tolist()
        print("Index created successfully")

    def exist_index(self, table, attr, num):
        if os.path.exists('index_info.npy'):
            info = np.load('index_info.npy', allow_pickle=True)
            info = info.tolist()
            for item in info:
                # delete the record in index_info.npy
                if item[0] == table and ''.join(item[1]) == attr:
                    # open the table file with index
                    index_name = item[2]
                    if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                        index_info = np.load(table + '_' + ''.join(attr) + '_' + index_name + '.npy', allow_pickle=True)
                        index_info = index_info.tolist()
                        hashtable = HashTable(7)
                        hash_num = hashtable.hash(num)
                        return index_info[hash_num]
            return None

    def drop_index(self, index_name, table):
        # check if index exists
        if os.path.exists('index_info.npy'):
            info = np.load('index_info.npy', allow_pickle=True)
            info = info.tolist()
            # check if the index was existed, index is the third attribute
            i = 0
            for item in info:
                # delete the record in index_info.npy
                if (item[2] == index_name):
                    attr = item[1]
                    info.remove(item)
                    i = i + 1
                    infoarr = np.array(info)
                    np.save('index_info.npy', infoarr)
                    # delete the index.npy file
                    if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                        print(table + '_' + ''.join(attr) + '_' + index_name + '.npy')
                        print(info)
                        os.remove(table + '_' + ''.join(attr) + '_' + index_name + '.npy')
                    else:
                        raise Exception('[ERROR]: The index does not exist')

        else:
            raise Exception('[ERROR]: The index does not exist')

    # insert index info during insert operation
    #remeber to call this function on each attributes
    def insert_index(self, attr, table, row, value):
        # check if index exists
        if os.path.exists('index_info.npy'):
            info = np.load('index_info.npy', allow_pickle=True)
            info = info.tolist()
            # check if the index was existed, index is the third attribute
            for item in info:
                if item[0] == table and ''.join(item[1]) == attr:
                    # got the information to find the index file
                    index_name = item[2]
                    if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                        # extract the information from the index file
                        alist = np.load(table + '_' + ''.join(attr) + '_' + index_name + '.npy', allow_pickle=True)
                        hashlist = alist.tolist()
                        #compute which hash dict should this row be
                        hashtable = HashTable(7)
                        hash_index = hashtable.hash(value)
                        hashlist[hash_index][value] = row
                        alist = np.array(hashlist)
                        print(alist)
                        if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                            np.save(table + '_' + ''.join(attr) + '_' + index_name + '.npy', alist)

    def delete_index(self, table, attr, value):
        if os.path.exists('index_info.npy'):
            info = np.load('index_info.npy', allow_pickle=True)
            info = info.tolist()
            for item in info:
                if item[0] == table and ''.join(item[1]) == attr:
                    index_name = item[2]
                    if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                        # extract the information from the index file
                        alist = np.load(table + '_' + ''.join(attr) + '_' + index_name + '.npy', allow_pickle=True)
                        hashlist = alist.tolist()
                        # compute which hash dict should this row be
                        hashtable = HashTable(7)
                        hash_index = hashtable.hash(value)
                        del hashlist[hash_index][value]
                        alist = np.array(hashlist)
                        print(alist)
                        if os.path.exists(table + '_' + ''.join(attr) + '_' + index_name + '.npy'):
                            np.save(table + '_' + ''.join(attr) + '_' + index_name + '.npy', alist)


    def insert(self, attrs: list, data: list) -> None:
        """
        Add data into self.data as a hash table.
        TODO:
        -Put data into self.data{} as hash table. Key is prmkvalue, and value is attvalue = [].
        -Use attribute_object.typecheck to check every value, and if the value is invalid, raise error. If not put into attvalu.
        -Check primary key value, if the value already in prmkvalue, raise error.
        -Print essential information
        """
        # TODO: typecheck?
        # TODO index add
        datainput = [data]
        newtemp = pd.DataFrame(datainput, columns=attrs)
        newdata = newtemp.iloc[0]
        i = 0
        for attr in attrs:
            self.insert_index(attr, self.name, newdata, data[i])
            i += 1
        prmkvalue = []
        attvalue = []
        if attrs==[]:
            # TODO: typecheck
            # Must enter full-attr values by order
            if len(data)!=len(self.attrls):
                raise Exception('[ERROR]: Full-attr values is needed')

            dat = data[::]
            # Get primary-key values
            for _ in range(len(self.primary)):
                prmkvalue.append(dat.pop(0))
            # the remaining data is attr data
            attvalue=dat

            # TODO: typecheck
            for i in data:
                value = data[i]
                attname = self.attrls[i]
                # typecheck()
                # If false, raise error in typecheck()
                # If true, nothing happens and continue
                # If unique, call self uniquecheck()
                if attname in self.uniqueattr.keys():
                    if value in self.uniqueattr[attname].keys():
                        raise Exception('[ERROR]: Unique attribute values are in conflict!  ' + attname + " : " + str(value))
                    self.uniqueattr[attname][value] = prmkvalue
                self.attrs[attname].typecheck(value)
                # If it is not unique, raise [ERROR] in the function
                # Else, continue

            # Hash data
            self.data[tuple(prmkvalue)]=attvalue
        else:
            # Reorder by the oder of self.attrs
            attrs_dict=dict()
            for name in self.attrls:
                attrs_dict[name]=None

            # Get primary-key values
            for name in self.primary:
                if name not in attrs:
                    raise Exception('[ERROR]: Primary key cannot be NULL.')
                prmkvalue.append(data[attrs.index(name)])

            for i in range(len(attrs)):
                value = data[i]
                attname = attrs[i]

                if attname in self.uniqueattr.keys():
                    if value in self.uniqueattr[attname].keys():
                        raise Exception('[ERROR]: Unique attribute values are in conflict!  ' + attname + " : " + str(value))
                    self.uniqueattr[attname][value] = prmkvalue
                #self.attrs[attname].typecheck(value)
                self.attrs[attname].typecheck(value)

                attrs_dict[attname] = value
             # Get primary-key values
            for name in self.primary:
                # Pop primary-key value from the full-attr dict
                attrs_dict.pop(name)
            # The remaining data is attr data
            attvalue=list(attrs_dict.values())

            # Hash data
            if tuple(prmkvalue) not in self.data.keys():
                self.datalist = self.datalist + [prmkvalue + attvalue]
                self.data[tuple(prmkvalue)] = attvalue
            else:
                raise Exception('[ERROR]: Primary key value collision')
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass

    def delete(self, table_name, where):
        if where == []:
            self.data = {}
            self.datalist = []
            for a in self.uniqueattr.keys():
                self.uniqueattr[a] = {}
            # self.BTree
        elif len(where) > 1:
            raise Exception('[ERROR]: Mutiple where conditions is coming soon')
        elif len(where) == 1:
            if where[0]['attr'] not in self.primary:
                raise Exception('[ERROR]: You should delete by one of the primary key!')
            else:
                # TODO !!!!!!!!!!!!!!!!!
                if where[0]['operation'] == '=':
                    value = where[0]['value']
                    try:
                        value = int(value)
                    except:
                        pass

                    del self.data[tuple([value])]

                    colindex = 0
                    keylist = list(self.attrs.keys())
                    # find which column (get column index)
                    while where[0]['attr'] != keylist[colindex]:
                        colindex += 1

                    # interate whole datalist, find the row the user want to delete
                    i = 0
                    deleteData = []
                    for item in self.datalist:
                        if item[colindex] == value:
                            deleteData = item
                            break
                        i += 1

                    del self.datalist[i]

                    colNameList = list(self.attrs.keys())
                    # print(colNameList.keys())
                    # TODO
                    # delete unique list
                    prmkColName = self.primary
                    for uniqAttName in self.uniqueattr.keys():
                        index = 0  # index of unique colname in whole attrName list
                        while uniqAttName != colNameList[index]:
                            index += 1
                        uniqValue = deleteData[index]
                        del self.uniqueattr[uniqAttName][uniqValue]

                    # TODO add delete index
                    i = 0
                    for attrname in colNameList:
                        self.delete_index(table_name, attrname, deleteData[i])

    # res = table.search('*', '=', False, ['id', 5], False)
    # tag: is str
    # 'condition': [  where[i]['attr'], where[i]['value']  ]
    def search(self, attr, sym, tag, condition, groupbyFlag):

        if self.flag == 0:
            self.df = pd.DataFrame(self.datalist, columns=self.attrls)
        operationList = {
            '=': 1,
            '>': 2,
            '>=': 3,
            '<': 4,
            '<=': 5,
            'LIKE': 6,
            'NOT LIKE': 7,
            '<>': 8
        }
        if len(sym) == 0:
            situation = 0
        else:
            situation = operationList[sym]
        if groupbyFlag:
            temp = self.group_by(condition[2], condition[3], attr, df)
        else:
            temp = self.df

        if situation == 0:  # no where
            if attr == ['*']:
                return temp
            else:
                return temp.loc[:, attr]

        if situation == 1:
            ###################
            # get index
            ##################
            gettable = self.exist_index(self.name, condition[0], condition[1])
            if gettable is not None:
                # has index
                templist = []
                for item in gettable.values():
                    templist.append(item)
                data = pd.DataFrame(templist, columns=self.df.columns)
                temp = data
            else:
                pass
            if tag:
                condition[1].replace('"', '')
                if attr == ['*']:
                    # print(temp)
                    return temp.loc[temp[condition[0]] == condition[1][1:-1]]
                #
                return temp.loc[temp[condition[0]] == condition[1][1:-1], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] == condition[1]]
            #
            return temp.loc[temp[condition[0]] == condition[1], attr]
        if situation == 2:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] > temp[condition[1]]]
                return temp.loc[temp[condition[0]] > temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] > condition[1]]
            return temp.loc[temp[condition[0]] > condition[1], attr]
        if situation == 3:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] >= temp[condition[1]]]
                return temp.loc[temp[condition[0]] >= temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] >= condition[1]]
            return temp.loc[temp[condition[0]] >= condition[1], attr]
        if situation == 4:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] < temp[condition[1]]]
                return temp.loc[temp[condition[0]] < temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] < condition[1]]
            return temp.loc[temp[condition[0]] < condition[1], attr]
        if situation == 5:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] <= temp[condition[1]]]
                return temp.loc[temp[condition[0]] <= temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] <= condition[1]]
            return temp.loc[temp[condition[0]] <= condition[1], attr]
        if situation == 8:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] != temp[condition[1]]]
                return temp.loc[temp[condition[0]] != temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] != condition[1]]
            return temp.loc[temp[condition[0]] != condition[1], attr]

        if situation == 6:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]].str.contains(temp[condition[1]])]
                return temp.loc[temp[condition[0]].str.contains(temp[condition[1]]), attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]].str.contains(condition[1])]
            return temp.loc[temp[condition[0]].str.contains(condition[1]), attr]
        if situation == 7:
            if tag:
                if attr == ['*']:
                    return temp.loc[~temp[condition[0]].str.contains(temp[condition[1]])]
                return temp.loc[~temp[condition[0]].str.contains(temp[condition[1]]), attr]
            if attr == ['*']:
                return temp.loc[~temp[condition[0]].str.contains(condition[1]), attr]
            return temp.loc[~temp[condition[0]].str.contains(condition[1]), attr]


    def group_by(self, agg, attr_gr, attr, df):
        """
        :param situation: calculation of group by
        :param attr_gr: the attrs for group
        :param attr: attrs for calculation
        :param df: dataframe for group by
        :return: a dataframe
        """
        agg_funcs = {
            'MAX': 0,
            'MIN': 1,
            'AVG': 2,
            'SUM': 3,
            'COUNT': 4
        }
        situation = agg_funcs[agg]

        if attr == '*':
            raise Exception('[ERROR]: Invalid search.')
        gb = df.groupby(attr_gr)
        if situation == 0:
            return gb[attr].max()
        if situation == 1:
            return gb[attr].min()
        if situation == 2:
            return gb[attr].mean()
        if situation == 3:
            return gb[attr].sum()
        if situation == 4:
            return gb[attr].value_counts()

    def table_join(self, table, attr):
        df1 = pd.DataFrame(self.data)
        df2 = pd.DataFrame(table.data)
        return pd.merge(df1, df2, on=attr)


# if __name__ == '__main__':
#     data = []
#     for i in range(100):
#         if i > 50:
#             data.append([i,2])
#         else:
#             data.append([i,1])
#     attr1 = {'name': 'id', 'type': 'INT', 'notnull': False, 'unique': False}
#     attr2 = {'name': 'num', 'type': 'INT', 'notnull': False, 'unique': False}
#     info = {'name': 'test', 'attrs': [attr1, attr2], 'primary': '', 'foreign': []}
#     table = Table(['id', 'num'], info)
#     table.df = pd.DataFrame(data, columns=['id', 'num'])
#     print(table.df)
#     res = table.search('*', '=', False, ['id', 5], False)
#     # print(res)

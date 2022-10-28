def exit(action):
    return {
        'query_keyword' : 'exit'
    }



"""
create 
    action[1]='DATABASE' or 'INDEX' or 'TABLE'
"""
def create(action):
    if action[1].upper() == 'DATABASE':
        return{
            'query_keyword' : 'create',
            'type' : 'database',
            'name' : action[2].lower()
        }

    elif action[1].upper() == 'INDEX':
        return create_index(action)


    elif action[1].upper() == 'TABLE':
        table_info = action[3:]
        table_name = action[2].lower()

        std_type=['CHAR', 'FLOAT', 'INT']

        # Get attributes
        # (column_name1 data_type not_null, column_name2 data_type null)
        table_attrs=[]
        while True:
            if '(' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace('(', ''))
            elif ')' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace(')', ''))
                break
            else:
                table_attrs.append(table_info.pop(0))
        # Each elements contains attrbute type constrain
        table_attrs=' '.join(table_attrs).split(',')

        # for each elem in table_attrs, get attribute name, types and constrains(null status and unique status)
        attrs=[]
        _type=[]
        null_status=[]
        unique_status=[]
        for i in table_attrs:
            tmp=i.strip().split(' ')
            attrs.append(tmp[0].lower())
            _type.append(tmp[1].upper())

            # There is null or/and unique status
            if len(tmp)==3: 
                # print(tmp)
                if tmp[2].upper()=='NOT_NULL' or tmp[2].upper()=='NULL':
                    null_status.append(tmp[2].upper())
                    unique_status.append('')

                elif tmp[2].upper()=='UNIQUE' or tmp[2].upper()=='NOT_UNIQUE':
                    unique_status.append(tmp[2].upper())
                    null_status.append('')
                        
            # Only have (attrs and type)
            elif len(tmp)==2:
                null_status.append('')
                unique_status.append('')
            elif len(tmp)==4:
                if tmp[2].upper()!='NOT_NULL' and tmp[2].upper()!='NULL': raise Exception('[ERROR]: Invalid syntax.')
                if tmp[3].upper()!='NOT_UNIQUE' and tmp[3].upper()!='UNIQUE': raise Exception('[ERROR]: Invalid syntax.')
                null_status.append(tmp[2].upper())
                unique_status.append(tmp[3].upper())
            else: raise Exception('[ERROR]: Invalid command.')
        # print(attrs)
        # print(_type)
        # print(null_status)
        # print(unique_status)

        # Get primary key
        primary_key=[]
        if table_info:
            if table_info[0].upper()=='PRIMARY':
                table_info.pop(0)   # Pop PRIMARY

                # Pop KEY
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        primary_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                primary_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('[ERROR]: Invalid command')
        # print(primary_key)
        foreign_key=[]
        if table_info:
            if table_info[0].upper()=='FOREIGN':
                table_info.pop(0)   # Pop foreign

                # Pop 'KEY'
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        foreign_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                foreign_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('[ERROR]: Invalid command.')

        ref_table=[]
        ref_column=[]
        ref_columns=[]
        if table_info:
            while table_info[0].upper()=='REFERENCES':
                table_info.pop(0)   # Pop REFERENCES

                # [ref_database, ref_table]
                ref_name=table_info.pop(0)# Table basename

                ref_table.append(ref_name)

                if '(' in table_info[0] and ')' in table_info[0]:
                    ref_column.append(table_info.pop(0).strip('() '))
                    ref_columns.append(ref_column)
                else:
                    while True:
                        if '(' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip('( ').lower())
                        elif ')' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip(') ').lower())
                            ref_columns.append(ref_column)
                            break
                        else:
                            ref_column.append(table_info.pop(0).strip().lower())
                if not table_info: break
        # TODO: Each reference should have a on_delete and on_update
        # print(table_info)
        on_delete='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':

                if table_info[1].upper()=='DELETE':
                    table_info.pop(0)    # Pop on
                    table_info.pop(0)    # Pop delete
                    on_delete=table_info.pop(0)

        # print(table_info)
        on_update='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':
                    if table_info[1].upper()=='UPDATE':
                        table_info.pop(0)    # Pop on
                        table_info.pop(0)    # Pop 'UPDATE'
                        on_update=table_info.pop(0)

        ref_info=[{'schema': ref_table, 'columns': ref_columns, 'on_delete': on_delete.upper(), 'on_update': on_update.upper()}]
        # print(ref_info)

        attrs_ls=[]
        for i in range(len(attrs)):
            if _type[i] not in std_type:
                raise Exception('[ERROR]: Invalid type.')
            attrs_ls.append({
                    'name':attrs[i],
                    'type': _type[i],
                    'notnull': 1 if null_status[i].upper()=='NOT_NULL' else 0,
                    'unique': 1 if unique_status[i].upper()=='UNIQUE' else 0,
            })
        # print(attrs_ls)
        
        foreignk = {}

        for attri in foreign_key:
            foreignk[attri] = ref_info[0]
            i += 1
        
        info={
            'name':table_name,
            'attrs':attrs_ls,
            'primary': primary_key,
            'foreign': foreignk,
        }

        return{
            'query_keyword' : 'create',
            'type' : 'table',
            'name' : table_name,
            'attrls' : attrs,
            'info' : info
        }

    else:
        raise Exception('[ERROR]: Invalid command. Help: create table/database X')

"""
drop
    action[1]='DATABASE' or 'INDEX' or 'TABLE'
"""
def drop(action):
    if action[1].upper() == 'DATABASE':
        return{
            'query_keyword' : 'drop',
            'type' : 'database',
            'name' : action[2].lower()
        }

    elif action[1].upper()=='INDEX':
        return drop_index(action)

    elif action[1].upper() == 'TABLE':
        return{
            'query_keyword' : 'drop',
            'type' : 'table',
            'table_name' : action[2].lower()
        }


"""
create 
    action[1]='INTO'
    pop 2 values: Insert  INTO
    
"""
def insert(action):
    if action[1].upper() == 'INTO':
        attrs = []
        data = []
        # pop 2 values: Insert  INTO
        action.pop(0)
        action.pop(0)
        # pop :table name  (save in table_name)
        table_name=action.pop(0).lower()

        if action[0].upper()=='VALUES':    # insert table values ()
            # No attrs, only values
            # Pop VALUE
            action.pop(0)
            if '(' in action[0]:
                for value in action:
                    elem=value
                    if '.' in value:
                        try:
                            value=float(value)
                        except:
                            raise Exception('[ERROR]: Invalid command')
                    elif "'" in value:
                        value=value.strip("()' ")
                    else:
                        try:
                            value=int(value)
                        except:
                            raise Exception('[ERROR]: Invalid command')

                    data.append(value)
                    if ')' in elem:
                        return{
                            'query_keyword' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }

            else:
                raise Exception('[ERROR]: Invalid command')
            raise Exception('[ERROR]: Invalid command')
        else:   # attrs and data

            # Get attrs
            if '(' in action[0]:
                count=0
                for elem in action:
                    if ')' in elem:
                        attrs.append(elem.strip('() ,'))
                        count+=1
                        break
                    attrs.append(elem.strip('() ,'))
                    count+=1
                for _ in range(count):
                    action.pop(0)
                if action==[]: 
                    raise Exception('[ERROR]: No data')
                # print(attrs)
                if len(attrs)!=len(set(attrs)): 
                    raise Exception('[ERROR]: Duplicated attributes')
                if action[0].upper()!='VALUES':
                    raise Exception('[ERROR]: Invalid command')
                action.pop(0)

            else:
                raise Exception('[ERROR]: Invalid command')

            if '(' in action[0]:
                for value in action:
                    elem=value
                    value=value.strip('() ,')
                    if '.' in value:
                        try:
                            value=float(value)
                        except:
                            raise Exception('[ERROR]: Invalid command')
                    elif "'" in value:
                        value=value.strip("()' ,")
                    else:
                        try:
                            value=int(value)
                        except:
                            print(value)
                            raise Exception('[ERROR]: Invalid command')


                    if ')' in elem:
                        data.append(value)
                        if len(attrs)!=len(data):
                            raise Exception('[ERROR]: Data length is not correponding to attributes.')

                        return{
                            'query_keyword' : 'insert',
                            'table_name' : table_name,
                            'attrs' : attrs,
                            'data' : data
                        }
                    data.append(value)
                raise Exception('[ERROR]: Invalid command')
                
            else:
                raise Exception('[ERROR]: Invalid command')

    else:
        raise Exception('[ERROR]: Invalid command. help: insert into ')


"""
select
    [SELECT] (DISTINCT)
    action[1]='INTO'
    pop 2 values: Insert  INTO

"""
def select(action):
    # TODO add join keyword
    key_words = ['FROM', 'WHERE', 'ORDER', 'GROUP', 'BY', 'INNER','ON','FULL','OUTER','LEFT','RIGHT','JOIN']

    # pop: 'SELECT'
    if action[0].upper()=='SELECT':
        action.pop(0)

    # Get distinct
    distinct_key=0
    if action[0].upper()=='DISTINCT':
        distinct_key=1
        action.pop(0)

    ####################
    # get * or colName
    ####################
    select_tokens=[]
    while action[0].upper() not in key_words:
        # split by , 
        # save in select_tokens
        select_tokens.append(action.pop(0).lower().strip(', '))
    # attrs_dict----->{'colName,colName...': 'NORMAL'}
    attrs_dict=parse_attrs(select_tokens)

    ####################
    # Get table names
    ####################
    # Table names
    tableNames_list=[]
    # print(action)
    if action.pop(0).upper()=='FROM':
        # go out when next token is in keywords or has no next token.
        while action[0].upper() not in key_words:
            tableNames_list.append(action.pop(0).lower().strip(', '))
            if not action:
                break
            if action==[]:
                break
    else: raise Exception('[ERROR]: need from')

    ######################################################################################
    # Join
    # SELECT * FROM b INNER JOIN A on b.name=A.name
    # join_expression,joinTableNames,join_expression
    # join_type=-1 no use
    # join_type=0 inner join
    # join_type=1 full outer join
    # join_type=2 left outer join
    # join_type=3 right outer join
    ######################################################################################
    join_type=-1
    join_expression = dict()
    # TODO INNER JOIN
    # inner join
    join_clause = []
    joinTableNames=[]
    if action:
        if action[0].upper() == 'INNER':
            action.pop(0)
            if action.pop(0).upper() == 'JOIN':
                # TODO get tabe name---> joinTableName
                while action[0].upper() not in key_words:
                    joinTableNames.append(action.pop(0).lower().strip(', '))
                    if not action:
                        break
                    if action == []:
                        break
                # TODO: must have on --->join_expression
                # get join condition
                if action.pop(0).upper() == 'ON':
                    while action[0].upper() not in key_words:
                        join_clause.append(action.pop(0).strip(', '))
                        if not action:
                            break
                    # TODO: get executable condition
                    conditions = reorder_where_clause(join_clause)
                    join_expression = parse_conditions(conditions)
                    # update join type
                    join_type = 0
                else:raise Exception('[ERROR]: Missing ON in command')
            else: raise Exception('[ERROR]: INNER JOIN help: SELECT *FROM b INNER JOIN A on b.name=A.name')

    # TODO full outer JOIN
    if action:
        if action[0].upper() == 'FULL':
            action.pop(0)
            if action.pop(0).upper() == 'OUTER':
                if action.pop(0).upper() == 'JOIN':
                    # TODO get tabe name---> joinTableName
                    while action[0].upper() not in key_words:
                        joinTableNames.append(action.pop(0).lower().strip(', '))
                        if not action:
                            break
                        if action == []:
                            break
                    # TODO: must have on --->join_expression
                    # get join condition
                    if action.pop(0).upper() == 'ON':
                        while action[0].upper() not in key_words:
                            join_clause.append(action.pop(0).strip(', '))
                            if not action:
                                break
                        # TODO: get executable condition
                        conditions = reorder_where_clause(join_clause)
                        join_expression = parse_conditions(conditions)
                        # update join type
                        join_type = 1
                else:
                    raise Exception('[ERROR]: help: SELECT * FROM b Left outer join A on b.name=A.name')
            else:
                raise Exception('[ERROR]: LEFT OUTER JOIN help: SELECT * FROM b Left outer join A on b.name=A.name')

    # TODO Left outer JOIN
    if action:
        if action[0].upper() == 'LEFT':
            action.pop(0)
            if action.pop(0).upper() == 'OUTER':
                if action.pop(0).upper() == 'JOIN':
                    # TODO get table name---> joinTableName
                    while action[0].upper() not in key_words:
                        joinTableNames.append(action.pop(0).lower().strip(', '))
                        if not action:
                            break
                        if action == []:
                            break
                    # TODO: must have on --->join_expression
                    # get join condition
                    if action.pop(0).upper() == 'ON':
                        while action[0].upper() not in key_words:
                            join_clause.append(action.pop(0).strip(', '))
                            if not action:
                                break
                        # TODO: get executable condition
                        conditions = reorder_where_clause(join_clause)
                        join_expression = parse_conditions(conditions)
                        # update join type
                        join_type = 2
                else:
                    raise Exception('[ERROR]: help: SELECT * FROM b Left outer join A on b.name=A.name')
            else:
                raise Exception('[ERROR]: LEFT OUTER JOIN help: SELECT * FROM b Left outer join A on b.name=A.name')
    # TODO Right outer JOIN
    if action:
        if action[0].upper() == 'RIGHT':
            action.pop(0)
            if action.pop(0).upper() == 'OUTER':
                if action.pop(0).upper() == 'JOIN':
                    # TODO get table name---> joinTableName
                    while action[0].upper() not in key_words:
                        joinTableNames.append(action.pop(0).lower().strip(', '))
                        if not action:
                            break
                        if action == []:
                            break
                    # TODO: must have on --->join_expression
                    # get join condition
                    if action.pop(0).upper() == 'ON':
                        while action[0].upper() not in key_words:
                            join_clause.append(action.pop(0).strip(', '))
                            if not action:
                                break
                        # TODO: get executable condition
                        conditions = reorder_where_clause(join_clause)
                        join_expression = parse_conditions(conditions)
                        # update join type
                        join_type = 3
                else:
                    raise Exception('[ERROR]: help: SELECT a, b FROM xt RIGHT outer join yt on xt.name=yt.name')
            else:
                raise Exception('[ERROR]: LEFT OUTER JOIN help: SELECT * FROM b RIGHT outer join A on b.name=A.name')
    ####################
    # Where clause
    # "select * from table A where id=5;"
    ####################
    where_clause=[]
    where_expression=dict()
    # conditions=[]
    # op=[]
    if action:
        if action[0].upper()=='WHERE':
            action.pop(0)
            while action[0].upper() not in key_words:
                where_clause.append(action.pop(0).strip(', '))
                if not action:
                    break
            conditions=reorder_where_clause(where_clause)

            # where clause poland expression
            where_expression=parse_conditions(conditions)   # Parse where clause
    # print('where clause: ', where_expression)

    # Get group by clause
    groupBy_clause=[]
    groupBy_expression=dict()
    if action:
        if action[0].upper()=='GROUP':
            action.pop(0)   # Pop group
            # pop BY
            if action.pop(0).upper()!='BY': raise Exception('[ERROR]: Invalid command.')
            while action[0].upper() not in key_words:
                groupBy_clause.append(action.pop(0).strip(', '))
                if not action:
                    break
            if groupBy_clause[0].upper()=='HAVING': raise Exception('[ERROR]: Invalid command.')

            groupBy_expression=parse_groupBy(groupBy_clause, attrs_dict)
    # print('GROUP BY CLAUSE: ', groupBy_expression)

    orderBy_clause=[]
    orderBy_expression=dict()
    if action:
        if action[0].upper()=='ORDER':
            action.pop(0)   # Pop order
            if action.pop(0).upper()!='BY':
                raise Exception('[ERROR]: Invalid command')
            while action[0].upper() not in key_words:
                orderBy_clause.append(action.pop(0).lower().strip(', '))
                if not action:
                    break
            orderBy_expression=parse_orderBy(orderBy_clause)
    # print('ORDER BY CLAUSE: ', orderBy_expression)
    if action!=[]:
        raise Exception('[ERROR]: Invalid command.')
    return {
        'query_keyword': 'select',
        'attrs': attrs_dict,    # dict->{attr: aggregate function, } such as {id: MAX, }
        'tables': tableNames_list,    # list->[table_names]
        'where': where_expression,  # list->[{attr: , value: , operation: , tag:}, op, ] Poland expression
        # dict->{group_by: [attrs], conditions: [Poland expression like where_clause]}
        'groupby': groupBy_expression,  
        'orderby': orderBy_expression,   # dict->{order_by: [attrs], order: DESC/ASC/NO_ACTION}
        'join_condition':join_expression,
        'joinTableNames':joinTableNames,
        'joinType':join_type
    }

def reorder_where_clause(where_clause):
    conditions=[]
    temp=[]
    op=[]
    # print(where_clause)
    for i in range(len(where_clause)):      
        condition=dict()
        if (where_clause[i].upper() in ['OR', 'AND', '(', ')'] and where_clause[i-2].upper()!='BETWEEN') or i==len(where_clause)-1:
            if i==len(where_clause)-1:
                temp.append(where_clause[i])
            else:
                op.append(where_clause[i].upper())
                if where_clause[i+1].upper() in ['OR', 'AND', '(', ')']:
                    continue                

            if temp:
                tag=0
                temp=' '.join(temp)
                if '<=' in temp:
                    tmp=temp.split('<=')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'operation': '<=', 'tag': tag}
                elif '>=' in temp:
                    tmp=temp.split('>=')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'operation': '>=', 'tag': tag}
                elif '<>' in temp:
                    if tmp[1][0]!="'" and tmp[1][len(tmp[1])-1]!="'":
                        tag=1
                    tmp=temp.split('<>')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1].strip("'"), 'operation': '<>', 'tag': tag}
                elif '=' in temp:
                    tmp=temp.split('=')
                    if tmp[1][0]!="'" and tmp[1][len(tmp[1])-1]!="'":
                        tag=1

                    value = tmp[1].strip('() ,')
                    try:
                        value=int(value)
                        tag=0
                    except:
                        tag=1

                    condition={'attr': tmp[0].lower(), 'value': value, 'operation': '=', 'tag': tag}
                elif '<' in temp:
                    tmp=temp.split('<')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'operation': '<', 'tag': tag}
                elif '>' in temp:
                    tmp=temp.split('>')
                    try:
                        value=float(tmp[1])
                    except:
                        value=tmp[1]
                        tag=1
                    condition={'attr': tmp[0].lower(), 'value': value, 'operation': '>', 'tag': tag}
                elif ' LIKE ' in temp.upper():
                    tmp=temp.split('LIKE')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1], 'operation': 'LIKE', 'tag': 0 }
                elif ' NOT LIKE ' in temp.upper():
                    tmp=temp.split('LIKE')
                    condition={'attr': tmp[0].lower(), 'value': tmp[1], 'operation': 'NOT LIKE', 'tag': 0 }
                elif 'BETWEEN' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower() #Pop attr
                    if tmp.pop(0).upper()!='BETWEEN': raise Exception('[ERROR]: Invalid Where Clause.')   #Pop Between
                    
                    try:
                        if float(tmp[0])>float(tmp[2]):
                            raise Exception('[ERROR]: Invalid where clause')
                    except: raise Exception('[ERROR]: Invalid where clause')

                    # v1
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'operation': '>=',
                        'tag': 0 
                    })

                    if tmp.pop(0).upper()!='AND': raise Exception('[ERROR]: Invalid where Clause.')   # Pop AND
                    conditions.append('AND')
                    # v2
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'operation': '<=',
                        'tag': 0 
                    })
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif 'BETWEEN' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower() #Pop attr
                    if tmp.pop(0).upper()!='BETWEEN': raise Exception('[ERROR]: Invalid where Clause.')   #Pop Between
                    
                    try:
                        if float(tmp[0])>float(tmp[2]):
                            raise Exception('[ERROR]: Invalid where clause')
                    except: raise Exception('[ERROR]: Invalid where clause')

                    # v1
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'operation': '<=',
                        'tag': 0 
                    })

                    if tmp.pop(0).upper()!='AND': raise Exception('[ERROR]: Invalid where Clause.')   # Pop AND
                    conditions.append('AND')
                    # v2
                    conditions.append({
                        'attr': tmp_attr,
                        'value': tmp.pop(0),
                        'operation': '>=',
                        'tag': 0 
                    })
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif ' IN ' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower()    # Pop attr
                    if tmp.pop(0).upper()!='IN': raise Exception('[ERROR]: Invalid where Clause.')  # Pop IN
                    tmp=','.join(tmp).strip('() ').split(',')
                    count=0
                    for val in tmp:
                        conditions.append({
                            'attr': tmp_attr,
                            'value': val.strip(', '),
                            'operation': '=',
                            'tag': 0 
                        })
                        count+=1
                        if count!=len(tmp):
                            conditions.append('OR')
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                elif ' NOT IN ' in temp.upper():
                    tmp=temp.split(' ')
                    tmp_attr=tmp.pop(0).lower()    # Pop attr
                    if tmp.pop(0).upper()!='IN': raise Exception('[ERROR]: Invalid where Clause.')  # Pop IN
                    tmp=','.join(tmp).strip('() ').split(',')
                    count=0
                    for val in tmp:
                        conditions.append({
                            'attr': tmp_attr,
                            'value': val.strip(', '),
                            'operation': '<>',
                            'tag': 0 
                        })
                        count+=1
                        if count!=len(tmp):
                            conditions.append('OR')
                    temp=[]
                    if op:
                        while op:
                            conditions.append(op.pop(0))
                    continue
                else: raise Exception('[ERROR]: Invalid where clause')
                conditions.append(condition)
                if op:
                    while op:
                        conditions.append(op.pop(0))
            # else: raise Exception('[ERROR]: Invalid where clause')
            temp=[]
        else:
            temp.append(where_clause[i])
    return conditions

def parse_attrs(attrs):
    # Input: list, aggragete attrs and normal attrs
    # Output: dict, key is attr, value is aggragation
    # ex. {id: max}
    # print(attrs)
    key_words=['MAX', 'MIN', 'AVG', 'COUNT', 'SUM']

    parse_attrs=dict()
    for attr in attrs:
        if '(' in attr and attr[-1]==')':
            temp=attr.split('(')
            # temp 0 is aggragate
            # temp 1 is attr
            agg_word=temp[0].strip('() ,').upper()
            attr_tmp=temp[1].strip('() ,').lower()
            if agg_word not in key_words: raise Exception('[ERROR]: Invalid command.')

            parse_attrs[attr_tmp]=agg_word
            
        elif '(' not in attr and ')' not in attr:
            parse_attrs[attr]='NORMAL'
        else: raise Exception('[ERROR]: Invalid command.')
    return parse_attrs

def parse_groupBy(groupBy_clause, attrs):
    att=attrs.copy()

    # TODO: Check attr and groupBy attr
    # print(groupBy_clause)
    groupBy=[]
    having=[]
    for i in range(len(groupBy_clause)):
        if groupBy_clause[i].upper()=='HAVING':
            having=groupBy_clause[i+1:]
            break
        groupBy.append(groupBy_clause[i])

    if len(groupBy)==len(attrs):
        if set(groupBy)!=set(attrs.keys()):
            raise Exception('[ERROR]: Invalid group by clause.')
        if len(set(attrs.values()))!=1:
            raise Exception('[ERROR]: Group by attrs cannot have aggregate function')
        if 'NORMAL' not in list(attrs.values()):
            raise Exception('[ERROR]: Group by attrs cannot have aggregate function')
    else:
        for elem in groupBy:
            del att[elem]
        if 'NORMAL' in list(att.values()):
            raise Exception('[ERROR]: Invalid group by clause')


    conditions=reorder_where_clause(having)
    expression=parse_conditions(conditions)

    return {
        'group_by': groupBy,
        'conditions': expression
    }

def parse_orderBy(orderBy_clause):
    # Input: list, order by clause
    # Output: dict, {order_by: attr, order: desc/asc/no_action}
    # print(orderBy_clause)
    key_words=['DESC', 'ASC']
    order_status='NO_ACTION'
    orderBy=[]

    if orderBy_clause[-1].upper() in key_words:
        order_status=orderBy_clause[-1].upper().strip()
        orderBy=orderBy_clause[: len(orderBy_clause)-1]
    else:
        orderBy=orderBy_clause

    return {
        'order_by': orderBy,
        'order': order_status
    }

def parse_conditions(con_ls):
    # Poland expression
    # op=['OR', 'AND']
    stack=[]
    ops=[]
    for item in con_ls:
        if str(item).upper() in ['OR', 'AND']:
            while len(ops)>=0:
                if len(ops)==0:
                    ops.append(item)
                    break
                op=ops.pop()
                if op=='(':
                    ops.append(op)
                    ops.append(item)
                    break
                else:
                    stack.append(op)
        elif item=='(':
            ops.append(item)
        elif item==')':
            while len(ops)>=0:
                op=ops.pop()
                if op=='(':
                    break
                else:
                    stack.append(op)
        else:
            stack.append(item)

    while len(ops)>0:
        stack.append(ops.pop())
    return stack

def save(action):
    return{
        'query_keyword' : 'save',
        'name' : action[-1]
    }

def use(action):
    return{
        'query_keyword' : 'use',
        'name' : action[-1]
    }

def show(action):
    if len(action) != 2:
        raise Exception('[ERROR] Invalid command. help: show databases/tables ')
    if action[1] == 'databases':
        return{
            'query_keyword' : 'show',
            'type' : 'database'
        }
    elif action[1] == 'tables':
        return{
            'query_keyword' : 'show',
            'type' :'table'
        }
    else:
        raise Exception('[ERROR] Invalid command. help: show databases/tables ')


def update(action):
    # TODO
    if action[0].upper() == 'UPDATE':
        action.pop(0)
    table_name = action.pop(0)
    if action.pop(0).upper() != 'SET': raise Exception('[ERROR]: Invalid command.')

    set_dict = []
    while action[0].upper() != 'WHERE':
        condition = action.pop(0).strip(', ')
        if '=' not in condition:
            raise Exception('[ERROR]: Invalid command.')
        tmp = condition.split('=')
        tmpvalue = tmp[1]
        if '.' in tmpvalue:
            try:
                tmpvalue = float(tmpvalue)
            except:
                raise Exception('[ERROR]: Invalid command')
        elif "'" in tmpvalue:
            tmpvalue = tmpvalue.strip("()' ")
        else:
            try:
                tmpvalue = int(tmpvalue)
            except:
                raise Exception('[ERROR]: Invalid command')

        set_dict.append({
            'attr': tmp[0].lower(),
            'value': tmpvalue,
        })
        if action == []:
            break

    where_expression = []
    if action:
        if action[0].upper() != 'WHERE':
            raise Exception('[ERROR]: Invalid command.')
        action.pop(0)  # Pop where
        conditions = reorder_where_clause(action)

        # where clause poland expression
        where_expression = parse_conditions(conditions)  # Parse where clause
    return {
        'query_keyword': 'update',
        'table': table_name,  # str->table name
        'set': set_dict,  # list->[{attr:, value:}]
        'where': where_expression,  # list-> like where clause
    }

def delete(action):
    if action[0].upper()=='DELETE':
        action.pop(0)
    if action.pop(0).upper()!='FROM':
        raise Exception('[ERROR]: Invalid command.')
    table_name=action.pop(0).lower()

    where_expression=[]
    if action:
        if action.pop(0).upper()!='WHERE':
            raise Exception('[ERROR]: Invalid command.')
        conditions=reorder_where_clause(action)

        # where clause poland expression
        where_expression=parse_conditions(conditions)   # Parse where clause
    return{
        'query_keyword': 'delete',
        'table': table_name,
        'where': where_expression,
    }

def create_index(action):
    if action[0].upper()=='CREATE':
        action.pop(0)
    if action.pop(0).upper()!='INDEX':
        raise Exception('[ERROR]: Invalid command.')

    idex_name=action.pop(0)

    if action.pop(0).upper()!='ON':
        raise Exception('[ERROR]: Invalid command.')
    table_name=action.pop(0)

    if '(' not in action[0] or ')' not in action[-1]:
        raise Exception('[ERROR]: Invalid command.')
    _str=' '.join(action).strip('() ')
    attrs=_str.split(',')
    attrs=list(map(str.strip, attrs))
    return {
        'query_keyword': 'create',
        'type': 'index',
        'index_name': idex_name,
        'table': table_name,
        'attrs': attrs,
    }

def drop_index(action):
    if action[0].upper()=='DROP':
        action.pop(0)

    if action.pop(0).upper()!='INDEX':
        raise Exception('[ERROR]: Invalid command.')
    idex_name=action.pop(0)

    if action.pop(0).upper()!='ON':
        raise Exception('[ERROR]: Invalid command.')

    table_name=action.pop(0)

    if action:
        raise Exception('[ERROR]: Invalid command.')

    return {
        'query_keyword': 'drop',
        'type': 'index',
        'index_name': idex_name,
        'table': table_name,
    }



query_list = {
    'EXIT' : exit,
    'CREATE' : create,
    'DROP' : drop,
    'INSERT' : insert,
    'SELECT' : select,
    'SAVE' : save,
    'SHOW' : show,
    'USE' : use,
    'UPDATE': update,
    'DELETE': delete,
}

# read query by using parser
def startParse(commandline):
    # copy commandline
    temp = commandline

    # split query by space
    action = commandline.split(' ')
    while True:
        if '' not in action:
            break
        action.remove('')
    # get the first word ---> change to upper case
    keyword = action[0].upper()
    validaction = ""
    for ac in query_list:
        validaction = validaction + " " + ac

    if keyword not in query_list:
        raise Exception('The query: ' + temp + ' Valid action:' + validaction)
        
    act = query_list[keyword](action)

    return act


# TEST
# action=startParse("SELECT * FROM b inner JOIN A on b.name=A.name where b.id=5;")
# print(action['where'].type)
# print(startParse("delete from test1 where a=2"))
# print(startParse('insert into test4 (a, b) values (100000, 100000)'))
# print(startParse('insert into test1 (a, b) values (2000, 23)'))
# 'create table students (id int not_null unique, name char, age int) primary key (id)'
# print(startParse('insert into students (id, name, age) values (1, \'jack\', 23)'))
# tableName='a'
# col1name='colName1'
# col2name='colName2'
# i=2
# demoQuery = 'insert into ' + tableName + ' (' + col1name + ', ' + col2name + ') values (' + str(i) + ', 1);\n'
# print(demoQuery)
# print(startParse(demoQuery))


# print(startParse("SELECT a,b FROM test0 INNER JOIN test2 on test0.a=test2.a where test0.a>5"))
# print(startParse("select * from table A where id=5;"))
# print(startParse("SELECT * FROM b full outer JOIN A on b.name=A.name where b.id=5;"))
# print(startParse("SELECT * FROM b inner JOIN A on b.name=A.name where b.id=5;"))
# print(startParse("CREATE TABLE ttest (id int not_null, a float not_null) PRIMARY KEY (id) FOREIGN KEY (id) REFERENCES Persons(id_p)"))
# print("create table ttest (id int not_null unique, a int unique) primary key (' + col1name + ');")
# print(startParse("SELECT * FROM test0 INNER JOIN test1 on test0.a=test1.a"))
# print(startParse("create table test0 (a int not_null unique, b int unique) primary key (a);"))

# TEST RESULT
# distinct:  0
# attrs:  {'*': 'NORMAL'}
# tables:  ['table', 'a']
# where clause:  [{'attr': 'id', 'value': '5;', 'operation': '=', 'tag': 1}]
# GROUP BY CLAUSE:  {}
# ORDER BY CLAUSE:  {}
# {'query_keyword': 'select', 'attrs': {'*': 'NORMAL'}, 'tables': ['table', 'a'], 'where': [{'attr': 'id', 'value': '5;', 'operation': '=', 'tag': 1}], 'groupby': {}, 'orderby': {}}

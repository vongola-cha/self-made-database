import pandas as pd

l1 = [[1, 'a'], [2, 'bb'], [3, 'a'], [5, 'a']]
l2 = ['id', 'name']
df = pd.DataFrame(l1, columns=l2)
# print(df)
row=df.iloc[1]
row2=df.iloc[2]
list=[]
list.append(row)
list.append(row2)
# print(row)
dftest=pd.DataFrame([row],columns=l2)
dftest=dftest.append(row2)
# print(dftest)
# print(df.columns.tolist())
# diction=dict()
# diction['a']=df
a=[1,[1,2],[3,4],4]
del a[2]
print(a)

# # iter
# lista=[1,2,4,5,3,63,1]
# for item in lista:
#     if item==2:
#         lista.remove(item)
# print(lista)
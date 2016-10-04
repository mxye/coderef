





#sort a pandas series in place:
#perhaps better to not sort in place, in case need to sort back by index
srs.sort_values(inplace=True, ascending=False)

#check data frame field column types:
df.dtypes

#pandas select rows
#see http://stackoverflow.com/questions/31593201/pandas-iloc-vs-ix-vs-loc-explanation
# loc works on labels in the index.
# iloc works on the positions in the index (so it only takes integers).
# ix usually tries to behave like loc but falls back to behaving like iloc if the label is not in the index.

#pandas change column/field value based on another column/field value:
df.loc[df['basedonfield'].str.contains("token"), 'changefield'] = "Token"
#select rows: first 10 rows
df.iloc[:10]

#get values of multiindex, for example the first level
df.index.levels[1].values

#get rows with one level of index equal to 'level_index_value'
df.xs('level_index_value',level='level_name')

#create column in pandas dataframe with multiindex
#you can use a function, or cal inline without function:
#if use a function, then if need multiple input columns from the same row, define it first
def newfunc (col1ofRow, col2ofRow, otherParam):
    return col1ofRow*col2ofRow/otherParam

l1name = 'colindexLevel1'
oP = 3

df[l1name, 'new_colindex_level_2', 'colindexLevel3'] = \
df.apply(lambda x: newfunc(x[l1name, \
    'existing_colindex_level_2', 'other_colindexLevel3'], \
    x[('', '', 'some_other_colindexLevel3')], oP), axis=1)

df[l1name, 'new_colindex_level_2', 'colindexLevel3'] = \
(someParam-df[l1name, 'existing_colindex_level_2', 'other_colindexLevel3']) \
*tr/dr/(tr-df['', '', 'some_other_colindexLevel3'])



#copy, clone or duplicate a list
new_list = old_list[:]
#or
new_list = list(old_list)
#or
#This is a little slower than list() because it has to find out the datatype of old_list first.
import copy
new_list = copy.copy(old_list)
#or, If the list contains objects and you want to copy them as well, use generic copy.deepcopy():
import copy
new_list = copy.deepcopy(old_list)




#pandas conditional creation of column
df['color'] = np.where(df['season']=='spring', 'green', 'grey')

#count distinct of a column
df.col_name.nunique()
#list all distinct values
df.name.unique()
#or
pd.unique(df.column_name.ravel())



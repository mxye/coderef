#replace segments of string by patterns
def regex_replace_all(text, dict):
    for i, j in dict.items():
        text = re.sub(i, j, text)
    return text

parse_dict = {'_out':''}
parse_dict2 = {'\.\w+':''}
temp = regex_replace_all(line['name'], parse_dict)


#pandas dataframe: drop columns
df.drop(['id','desc'], axis=1, inplace=True)


#merge dataframes in pandas:
#how being outer would be an outer join
mergedf = pd.merge(left=filedf,right=datadf, on='fname', how='outer')

#pandas dataframe interate rows:
row_iter = mergedf.iterrows()
for i, row in row_iter:
    print(i)

#pandas series iterate items:
for i, item in series.iteritems():
    print(i)


#replace one dash with empty but not two or more dashes?
#two passes: replace two or more dashes with something else
#and then replace one dash with empty

#pandas groupby
#group size or length:
gsize = df.groupby('fieldname').size()

#to do something to each GROUP, here the series from "anotherfield", use agg
wbycat = df.groupby('cat').anotherfield.agg(lambda x: [val for sublist in x.tolist() for val in sublist])
#see the words in the "Cat1" category
wbycat.Cat1
#do something to each row of a df: use apply
#the df in question can be the result of a groupby, and a field of the df could be, e.g. a Counter
#can define the lambda function beforehand
new_func = lambda x: list(set(x))
df['new_field'] = df['old_field'].apply(new_func)
#or apply the lambda function directly
#turn each group into counter
cbycat = wbycat.apply(lambda x: Counter(x))
#turn each group into dataframe
dbycat = cbycat.apply(lambda x: pd.DataFrame.from_dict(x, orient='index').reset_index())

#first, stack all dfs onto each other from each group
#then pivot to have them reshaped from long to wide
dflist = []
for ind,value in dbycat.iteritems():
    #this alters dbycat in place
    value['cat'] = ind
    #value['nres'] = nres[ind]
    dflist.append(value)

mcat = pd.concat(dflist, ignore_index=True)
del msedf
mcat.columns = ['phrase', 'count', 'cat']
#mcatm = pd.melt(mcat, id_vars=['phrase', 'cat'], value_vars='count')
#df.merge(mcat, on='index', how = 'outer')
mcatp = mcat.pivot(index='phrase', columns='cat', values='count')

#pandas change column/field value based on another column/field value:
df.loc[df['basedonfield'].str.contains("token"), 'changefield'] = "Token"

#subset a dataframe based on a column value is in a list
subdf = df[df['col_name'].isin(alist)]

#pandas multiple levels of column headers/indices
#rename column names
df.columns = ['c1', 'c2', 'c3', 'c4']
df.set_index(['c3', 'c4','c1'], inplace=True)
#unstack level -1 is the most immediate level, in this case c1
#0 and 1 are the next two levels
#reshape long to wide by unstacking using them both, i.e. c4 and c3
dfus = df.unstack(level=[0, 1])
#select a column
dfus['c2']['a_value_of_c3']['a_value_of_c4']

#if the resulting nested levels or headers are not ideal, you can swap levels
#eg between the 1st and the 3rd level:
dfus_new = dfus.swaplevel(2, 0, axis=1)

#see column multi-index here
dfus.columns.get_values()
#realize the column index is a tuple of ('', '', 'col_name'):
#i.e. the first two levels of multi-index is empty
#to select such a column
df[('', '', 'col_name')]


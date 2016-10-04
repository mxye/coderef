
#dump multiple dictionaries to one json file, one on each line:
#use json.dumps(dictionary) + '\n' and write to file line by line
with open(datapath+"result.json", 'w') as wf:
    for file in glob.glob(filename):
        with open(file, 'r') as fp:
            content = fp.read()
            linedict = {'name': file, 'content': content}
            wf.write(json.dumps(linedict) + '\n')



#convert a list of dictionaries into a pandas dataframe
datadf = pd.DataFrame(datalist)

#save a pandas dataframe as a csv file
df.to_csv(datapath+'df.csv')

#read and convert a csv file into a pandas dataframe
#keep id as string instead of integer, for later dataframe merging on id
df = pd.read_csv(datapath+'source.csv', dtype={'id': object})
#make the first column into an index column
adf = pd.read_csv(datapath+'adf.csv', index_col=0)
#read csv into pandas df with column 2 as date or datetime column:
df = pd.read_csv(datapath+'df.csv', index_col=0, parse_dates = [2])
#then convert the pandas timestamp to datetime, also extract year:
df['adate'] = pd.to_datetime(df['date_field_name'])
df['year'] = df.adate.dt.year




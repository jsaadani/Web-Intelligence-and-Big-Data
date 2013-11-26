#!/usr/bin/env python
import mincemeat
import glob


def file_contents(file_name):
    f=open(file_name)
    try:
        return f.read()
    finally:
        f.close()
        
# The data source can be any dictionary-like object
text_files=glob.glob("hw3data/*")
datasource=dict((file_name,file_contents(file_name)) for file_name in text_files)


def mapfn(k, v): #for each line
    from stopwords import allStopWords
    import string
    import re
    for line in v.splitlines():
        lineSplit=line.split(":::")
        title=lineSplit[2]
        words=title.lower().split()
        authors=lineSplit[1].split("::")
        for author in authors:
            for word in words:
                if len(word)>1:
                    if word not in allStopWords.keys():
                        word=re.sub("-"," ",word)
                        word = word.translate(None, string.punctuation)
                        yield author,word
    
    
def reducefn(k, vs):#for each author
    from collections import Counter
    return Counter(vs)
        

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="password")
#print results
resultline = str(results).split("),")
f = open('outfile.txt','w')
for result in resultline:
    f.write(str(result)+'),\n')
f.close()
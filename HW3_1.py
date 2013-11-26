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
text_files=glob.glob("hw3data/c0001")
datasource=dict((file_name,file_contents(file_name)) for file_name in text_files)


def mapfn(k, v): #for each line
    for line in v.splitlines():
        lineSplit=line.split(":::")
        title=lineSplit[2]
        authors=lineSplit[1].split("::")
        for author in authors:
            yield author,title
    
def reducefn(k, vs):#for each author
    from stopwords import allStopWords
    result=" ".join(vs)
    words=result.lower().split()
    cleanTitle=[]
    for w in words:
        if w not in allStopWords.keys():
            cleanTitle.append(w)
    for w in cleanTitle:
        return w,cleanTitle.count(w)
        

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="Audencia10")
print results
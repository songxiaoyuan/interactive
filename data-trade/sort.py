import sys, csv , operator  
#!/usr/bin/python
# -*- coding:utf8 -*-

import os
allFileNum = 0
def sort(filename):
    data = csv.reader(open(filename),delimiter=',')  
    sortedlist = sorted(data, key = lambda x: (x[20], int(x[21])))  
    with open(filename, "wb") as f:  
        fileWriter = csv.writer(f, delimiter=',')  
        for row in sortedlist:  
            fileWriter.writerow(row)  
    f.close()

def printPath(level, path):
    global allFileNum
    dirList = []
    fileList = []
    files = os.listdir(path)
    dirList.append(str(level))
    for f in files:
        if(os.path.isdir(path + '\\' + f)):
            if(f[0] == '.'):
                pass
            else:
                dirList.append(f)
        if(os.path.isfile(path + '\\' + f)):
            fileList.append(f)
    i_dl = 0
    for dl in dirList:
        if(i_dl == 0):
            i_dl = i_dl + 1
        else:
            print '-' * (int(dirList[0])), dl
            printPath((int(dirList[0]) + 1), path + '\\' + dl)
    i=1
    for fl in fileList:
        print i
        # i=i+1
        # if i<=126:
        #     continue
        #     pass

        sort(path+'\\'+fl)
        print fl
        allFileNum = allFileNum + 1

if __name__ == '__main__':
    printPath(1, 'D:\data\optiona') 


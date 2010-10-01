import sys
from fabulous.color import *
     

def fitTo(s,n):
    if len(s) >= n:
        return s[:n-1] + " "
    else:
        return s + " "*(n-len(s))


def goodSize(l):    
    return  max(3,min(max(len(s) for s in l)+1,80))

def column(d,n):
    return [c[n] for c in d]


def fitSizes(sizes,width):
    while sum(sizes) > width:
        maxsize = max(sizes)
        sizes = [min(size,maxsize-1) for size in sizes] 
    return sizes

def display(d,width=None):
    if d:
        sizes = [goodSize(column(d,i)) for i in range(len(d[0]))]
        if width:
            sizes = fitSizes(sizes,width)
        for r in d:
            for l,s in zip(r,sizes):
                 sys.stdout.write(fitTo(l,s))
            sys.stdout.write("\n")

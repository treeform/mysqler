from collections import defaultdict


ios = defaultdict(list)

for d in open("data.txt"):
    id,io,l = d.split(',')
    id,io,l = int(id),int(io),int(l)
    ios[io].append((id,l))

for io,data in ios.iteritems():
    ids = [d[1] for d in data]
    if len(ids)>1:
        if [d[1] for d in data if d[1] == 0]:
            print "IO:",io
            for d in data:
                print " rule id:",d[0],"li:",d[1]
        
    
    






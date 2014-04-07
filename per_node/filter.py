f = open('edges.txt')
o = open('edges_ot55.txt','w')
rejectMin = 0.99*0.55
rejectLimit = rejectMin+0.01
l = f.readline()
while l:
    u, v, p = [float(c.strip()) for c in l.split(' ') if c != '']
    if p < rejectMin or p > rejectLimit:
        o.write(l)
    l = f.readline()
o.close()
f.close()

f = open('blocks.txt')
blocks = [int(line.strip())-1 for line in f]
f.close()
f = open('output')
o = open('NodePageRank.txt', 'w')
for line in f:
    s = line.split()
    if int(s[1]) in blocks:
        o.write(s[1]+' '+s[2]+'\n')
o.close()
f.close()

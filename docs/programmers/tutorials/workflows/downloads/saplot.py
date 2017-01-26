#! /usr/bin/env python
"""
Make a line plot all price paths produced by `simAsset.R`,
together with their average value at any given time.
"""
import csv

import matplotlib.pyplot as plt


plt.style.use('ggplot')

data = open('results.csv')
rows = csv.reader(data)
ys = []
max_y = 0
for row in rows:
    y = list(float(item) for item in row)
    max_y = max(max_y, max(y))
    ys.append(y)

fig = plt.figure()

# plot "hairy"
for y in ys:
    x = range(len(y))
    plt.plot(x, y, linestyle='solid', color='chartreuse', alpha=(1.0/8))

avgs = []
ts = zip(*ys)
N = len(ys)
for t in ts:
    avg = sum(t) / N
    avgs.append(avg)
plt.plot(x, avgs,  linestyle='solid', linewidth=2, color='darkred', alpha=1.0)

plt.ylim(0, max_y)
#plt.show()

fig.savefig("saplot.pdf")

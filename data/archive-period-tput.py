import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.set_xlabel('Archive Period (s)')
ax.set_ylabel('Throughput (ops/sec)')
fig.savefig('data/archive-period-tput.png')
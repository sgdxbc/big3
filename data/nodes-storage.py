import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.set_xlabel('The number of nodes')
ax.set_ylabel('Storage (GB)')
fig.savefig('data/nodes-storage.png')
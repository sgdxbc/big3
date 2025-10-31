import polars as pl
import matplotlib.pyplot as plt

df = pl.read_csv("data/nodes-tput.csv").filter(pl.col("_notes").is_null())
print(df)

fig, ax = plt.subplots()
for setting in df["setting"].unique():
    setting_df = df.filter(pl.col("setting") == setting)
    ax.plot(
        setting_df["num_nodes"],
        setting_df["tput"],
        marker="o",
        label=setting,
    )
ax.legend()
ax.set_xlim(0, None)
ax.set_ylim(0, None)
ax.set_xlabel("Number of Nodes")
ax.set_ylabel("Throughput (ops/sec)")
ax.set_title("Throughput vs Number of Nodes")
ax.grid(True)
fig.savefig("data/nodes-tput.png")

import polars as pl
import matplotlib.pyplot as plt

df = (
    pl.read_csv("data/nodes-tput.csv")
    .filter(pl.col("_notes").is_null())
    .with_columns((pl.col("num_faulty_nodes") * 3 + 1).alias("num_nodes"))
)
print(df)

fig, ax = plt.subplots()
ax.plot(
    df["num_nodes"],
    df["tput"],
    marker="o",
    label="Throughput",
)
ax.set_xlim(0, None)
ax.set_ylim(0, None)
ax.set_xlabel("Number of Nodes")
ax.set_ylabel("Throughput (ops/sec)")
ax.set_title("Throughput vs Number of Nodes")
ax.grid(True)
fig.savefig("data/nodes-tput.png")

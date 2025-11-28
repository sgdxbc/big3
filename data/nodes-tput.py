import polars as pl
import matplotlib.pyplot as plt


def plot(ax, df, network, app):
    # print(df)
    plot_df = df.filter((pl.col("network") == network) & (pl.col("app") == app))
    for setting in sorted(plot_df["setting"].unique()):
        setting_df = plot_df.filter((pl.col("setting") == setting))
        ax.plot(
            setting_df["num_nodes"],
            setting_df["tput"],
            marker="o",
            label=f"{setting}",
        )
    ax.legend()
    ax.set_ylim(0, None)
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title(f"{app}")
    ax.grid(True)


fig, axs = plt.subplots(1, 2, figsize=(12, 5))
df = pl.read_csv("data/nodes-tput.csv")  # .filter(pl.col("_ignore").is_null())
print(df)
plot(axs[0], df, "Lan", "Ycsb")
plot(axs[1], df, "Lan", "Utxo")
fig.savefig("data/nodes-tput.png")
import polars as pl
import matplotlib.pyplot as plt


def plot(ax, df, network, app, ylim=None):
    # print(df)
    plot_df = df.filter((pl.col("network") == network) & (pl.col("app") == app))
    for setting in sorted(plot_df["setting"].unique()):
        setting_df = plot_df.filter((pl.col("setting") == setting))
        ax.plot(
            setting_df["tput"],
            setting_df["p99"],
            marker="o",
            label=f"{setting}",
        )
    ax.legend()
    # ax.set_xlim(0, 500_000)
    # ax.set_ylim(0, None)
    ax.set_ylim(0, ylim)
    ax.set_xlabel("Throughput (ops/s)")
    ax.set_ylabel("P99 Latency (s)")
    ax.set_title(f"{network} - {app}")
    ax.grid(True)


fig, axs = plt.subplots(2, 2, figsize=(12, 10))
df = pl.read_csv("data/tput-latency.csv")  # .filter(pl.col("_ignore") != True)
print(df)
plot(axs[0][0], df, "Lan", "Ycsb", ylim=1.2)
plot(axs[0][1], df, "Lan", "Utxo", ylim=1.2)
plot(axs[1][0], df, "Wan", "Ycsb", ylim=15)
plot(axs[1][1], df, "Wan", "Utxo", ylim=15)
fig.savefig("data/tput-latency.png")

import polars as pl
import matplotlib.pyplot as plt


def plot(ax, df, network, app):
    # print(df)
    plot_df = df.filter((pl.col("network") == network) & (pl.col("app") == app))
    for setting in sorted(plot_df["setting"].unique()):
        setting_df = plot_df.filter((pl.col("setting") == setting))
        ax.plot(
            setting_df["skewness"],
            setting_df["tput"],
            marker="o",
            label=f"{setting}",
        )
    ax.legend()
    ax.set_ylim(0, None)
    ax.set_xlabel("Skewness")
    ax.set_ylabel("Throughput (ops/s)")
    ax.grid(True)


fig, axs = plt.subplots(1, 1, figsize=(6, 5))
df = pl.read_csv("data/skew-tput.csv")  # .filter(pl.col("_ignore").is_null())
print(df)
plot(axs, df, "Lan", "Ycsb")
fig.savefig("data/skew-tput.png")
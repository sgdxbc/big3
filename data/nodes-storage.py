import polars as pl
import matplotlib.pyplot as plt


def plot(ax, df, network, app):
    # aggregate mean and std per (setting, num_nodes)
    plot_df = (
        df.filter((pl.col("network") == network) & (pl.col("app") == app))
        .group_by(["setting", "num_nodes"])  # changed: use Polars group_by
        .agg(
            [
                pl.col("storage").mean().alias("mean"),
                pl.col("storage").std().alias("std"),
            ]
        )
        .with_columns(pl.col("std").fill_null(0))
    )
    print(plot_df)
    # iterate settings and plot mean with error bars
    settings = sorted(plot_df["setting"].unique().to_list())  # changed: get unique settings
    for setting in settings:
        setting_df = plot_df.filter(pl.col("setting") == setting).sort("num_nodes")
        ax.errorbar(
            setting_df["num_nodes"].to_list(),
            setting_df["mean"].to_list(),
            yerr=setting_df["std"].to_list(),
            marker="o",
            label=f"{setting}",
            capsize=3,
            linestyle="-",
        )
    ax.legend()
    # ax.set_ylim(0, None)
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Storage (bytes)")
    ax.set_yscale("log")
    ax.grid(True)


fig, axs = plt.subplots(1, 1, figsize=(6, 5))
df = pl.read_csv("data/nodes-storage.csv")  # .filter(pl.col("_ignore").is_null())
print(df)
plot(axs, df, "Lan", "Ycsb")
fig.savefig("data/nodes-storage.png")
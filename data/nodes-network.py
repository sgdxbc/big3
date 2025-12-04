import polars as pl
import matplotlib.pyplot as plt
import numpy as np


def plot(ax, df, skewness):
    # aggregate mean and std per num_nodes for the three components (no setting)
    plot_df = df.filter(pl.col("skewness") == skewness)
    agg_df = (
        plot_df.group_by("num_nodes")
        .agg(
            [
                pl.col("consensus").mean().alias("consensus_mean"),
                pl.col("consensus").std().alias("consensus_std"),
                pl.col("fetch").mean().alias("fetch_mean"),
                pl.col("fetch").std().alias("fetch_std"),
                pl.col("checkpoint").mean().alias("checkpoint_mean"),
                pl.col("checkpoint").std().alias("checkpoint_std"),
            ]
        )
        .sort("num_nodes")
    )

    if agg_df.is_empty():
        return

    num_nodes = agg_df["num_nodes"].to_list()
    n = len(num_nodes)
    indices = np.arange(n)
    bar_width = 0.25

    cons_mean = agg_df["consensus_mean"].to_list()
    cons_std = agg_df["consensus_std"].to_list()
    fetch_mean = agg_df["fetch_mean"].to_list()
    fetch_std = agg_df["fetch_std"].to_list()
    chk_mean = agg_df["checkpoint_mean"].to_list()
    chk_std = agg_df["checkpoint_std"].to_list()

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    # grouped bars with error bars
    pos_cons = indices - bar_width
    pos_fetch = indices
    pos_chk = indices + bar_width

    ax.bar(pos_cons, cons_mean, width=bar_width, yerr=cons_std, color=colors[0], capsize=3, label="consensus")
    ax.bar(pos_fetch, fetch_mean, width=bar_width, yerr=fetch_std, color=colors[1], capsize=3, label="fetch")
    ax.bar(pos_chk, chk_mean, width=bar_width, yerr=chk_std, color=colors[2], capsize=3, label="checkpoint")

    # xticks: show actual num_nodes values
    ax.set_xticks(indices)
    ax.set_xticklabels([str(nv) for nv in num_nodes])

    ax.legend()
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Egress Traffic (bytes)")
    ax.grid(True)
    ax.set_title(f"Skewness = {skewness}")


fig, axs = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
df = pl.read_csv("data/nodes-network.csv")  # .filter(pl.col("_ignore").is_null())
print(df)
plot(axs[0], df, 0.99)
plot(axs[1], df, 1.24)
fig.savefig("data/nodes-network.png")

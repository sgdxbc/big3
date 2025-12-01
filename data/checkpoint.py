import polars as pl
import matplotlib.pyplot as plt
import numpy as np


def plot(ax, df, x, xlabel, k):
    # aggregate mean per num_nodes for checkpoint subcomponents and compute 'other'
    agg_df = (
        df.group_by(x)
        .agg(
            [
                pl.col("checkpoint").mean().alias("checkpoint"),
                pl.col("checkpoint_scan").mean().alias("checkpoint_scan"),
                pl.col("checkpoint_network").mean().alias("checkpoint_network"),
                pl.col("checkpoint_verify").mean().alias("checkpoint_verify"),
                pl.col("checkpoint_update").mean().alias("checkpoint_update"),
            ]
        )
        .sort(k)
        .with_columns(
            (
                pl.col("checkpoint")
                - (
                    pl.col("checkpoint_scan")
                    + pl.col("checkpoint_network")
                    + pl.col("checkpoint_verify")
                    + pl.col("checkpoint_update")
                )
            )
            # .clip_min(0)  # ensure non-negative
            .alias("checkpoint_other")
        )
    )
    print(agg_df)

    if agg_df.is_empty():
        return

    xs = agg_df[x].to_list()
    y_scan = agg_df["checkpoint_scan"].to_list()
    y_network = agg_df["checkpoint_network"].to_list()
    y_verify = agg_df["checkpoint_verify"].to_list()
    y_update = agg_df["checkpoint_update"].to_list()
    y_other = agg_df["checkpoint_other"].to_list()

    component_names = [
        "checkpoint_scan",
        "checkpoint_network",
        "checkpoint_verify",
        "checkpoint_update",
        "checkpoint_other",
    ]
    component_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

    # stacked area
    ax.stackplot(
        xs,
        [y_scan, y_network, y_verify, y_update, y_other],
        colors=component_colors,
        alpha=0.7,
        labels=[n.removeprefix("checkpoint_") for n in component_names],
    )

    # cumulative tops for overlay markers
    y1 = np.array(y_scan)
    y2 = y1 + np.array(y_network)
    y3 = y2 + np.array(y_verify)
    y4 = y3 + np.array(y_update)
    y5 = y4 + np.array(y_other)

    ax.plot(xs, y1, color=component_colors[0], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(xs, y2, color=component_colors[1], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(xs, y3, color=component_colors[2], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(xs, y4, color=component_colors[3], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(xs, y5, color=component_colors[4], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.legend()
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Time (s)")
    ax.grid(True)


fig, axs = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
df = pl.read_csv("data/checkpoint.csv")  # .filter(pl.col("_ignore").is_null())
print(df)
plot(axs[0], df.filter(pl.col("num_nodes") == 100), "num_keys", "Number of Keys", "num_keys")
plot(axs[1], df.filter(pl.col("num_keys") == 100_000_000), "num_nodes", "Number of Nodes", "num_nodes")
fig.savefig("data/checkpoint.png")

import polars as pl
import matplotlib.pyplot as plt
import numpy as np


def plot(ax, df, skewness):
    # aggregate mean per num_nodes for the three components (no setting)
    plot_df = df.filter(pl.col("skewness") == skewness)
    agg_df = (
        plot_df.group_by("num_nodes")  # changed: use correct .groupby
        .agg(
            [
                pl.col("consensus").mean().alias("consensus"),
                pl.col("fetch").mean().alias("fetch"),
                pl.col("checkpoint").mean().alias("checkpoint"),
            ]
        )
        .sort("num_nodes")
    )

    if agg_df.is_empty():
        return

    x = agg_df["num_nodes"].to_list()
    y_consensus = agg_df["consensus"].to_list()
    y_fetch = agg_df["fetch"].to_list()
    y_checkpoint = agg_df["checkpoint"].to_list()

    component_names = ["consensus", "fetch", "checkpoint"]
    component_colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    ax.stackplot(
        x,
        [y_consensus, y_fetch, y_checkpoint],
        colors=component_colors,
        alpha=0.6,
        labels=component_names,
    )

    # compute cumulative tops for each stacked layer
    y1 = np.array(y_consensus)
    y2 = y1 + np.array(y_fetch)
    y3 = y2 + np.array(y_checkpoint)

    # overlay lines with dot markers at the top of each stacked layer
    ax.plot(x, y1, color=component_colors[0], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(x, y2, color=component_colors[1], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")
    ax.plot(x, y3, color=component_colors[2], marker="o", linestyle="-", linewidth=1, markersize=4, label="_nolegend_")

    # legend for components
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

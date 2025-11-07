import polars as pl
import matplotlib.pyplot as plt


def plot(df, postfix):
    fig, ax = plt.subplots()
    print(df["network", "setting"].unique())
    for network, setting in df["network", "setting"].unique().iter_rows():
        setting_df = df.filter(
            (pl.col("network") == network) & (pl.col("setting") == setting)
        )
        ax.plot(
            setting_df["num_nodes"],
            setting_df["tput"],
            marker="o",
            label=f"{network}-{setting}",
        )
    ax.legend()
    ax.set_xlim(0, None)
    ax.set_ylim(0, None)
    # ax.set_ylim(0, 250_000)
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Throughput vs Number of Nodes")
    ax.grid(True)
    fig.savefig(f"data/nodes-tput-{postfix}.png")


df = pl.read_csv("data/nodes-tput-*.csv").filter(pl.col("_notes").is_null())
print(df.filter(pl.col("app") == "Utxo"))
plot(df.filter(pl.col("app") == "Utxo"), "utxo")
print(df.filter(pl.col("app") == "Ycsb"))
plot(df.filter(pl.col("app") == "Ycsb"), "ycsb")

import polars as pl
import matplotlib.pyplot as plt


def plot(df, postfix, ylim=None):
    # print(df)
    fig, ax = plt.subplots()
    print(df["app", "setting"].unique())
    for app, setting in df["app", "setting"].unique().iter_rows():
        setting_df = df.filter((pl.col("app") == app) & (pl.col("setting") == setting))
        ax.plot(
            setting_df["tput"],
            setting_df["p99"],
            marker="o",
            label=f"{app}-{setting}",
        )
    ax.legend()
    ax.set_xlim(0, 150_000)
    # ax.set_ylim(0, None)
    ax.set_ylim(0, ylim)
    ax.set_xlabel("Throughput (ops/s)")
    ax.set_ylabel("P99 Latency (s)")
    ax.set_title("P99 Latency vs Throughput")
    ax.grid(True)
    fig.savefig(f"data/tput-latency-{postfix}.png")


df = pl.read_csv("data/tput-latency-*.csv")  # .filter(pl.col("_ignore") != True)
print(df)
plot(df.filter((pl.col("network") == "Lan")), "lan", ylim=1.5)
plot(df.filter((pl.col("network") == "Wan")), "wan", ylim=15)

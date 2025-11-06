import polars as pl

server_df = pl.read_csv("data/servers-2020-07-19.csv").sample(
    100, seed=42, shuffle=True
)
print(server_df)

ping_df = pl.read_csv("data/pings-2020-07-19-2020-07-20.csv")

# def avg_latency()
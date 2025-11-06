import polars as pl

server_df = pl.read_csv("data/servers-2020-07-19.csv")
print(server_df)

ping_df = (
    pl.read_csv("data/pings-2020-07-19-2020-07-20.csv")
    .group_by(["source", "destination"])
    .agg(pl.col("avg").mean().alias("global_avg"))
)
print(ping_df)


def matrix():
    for id1 in server_df["id"]:

        def line():
            for id2 in server_df["id"]:
                if id1 == id2:
                    yield 0
                else:
                    avg = ping_df.filter(
                        (pl.col("source") == id1) & (pl.col("destination") == id2)
                    )["global_avg"]
                    assert len(avg) == 1, f"Missing ping from {id1} to {id2}"
                    yield int(avg[0])

        yield list(line())


latency_matrix = list(matrix())
print(latency_matrix)

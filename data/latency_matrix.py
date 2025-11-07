import polars as pl

N = 80

server_df = pl.read_csv("data/servers-2020-07-19.csv").sample(
    n=N, seed=0, shuffle=True
)
print(server_df)

ping_df = (
    pl.read_csv("data/pings-2020-07-19-2020-07-20.csv")
    .group_by(["source", "destination"])
    .agg(pl.col("avg").mean().alias("global_avg"))
)
print(ping_df)
all_avg = int(ping_df["global_avg"].mean())
print(f"Overall average latency: {all_avg}ms")


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
                    assert len(avg) <= 1
                    if len(avg) == 0:
                        # print(f"Missing ping from {id1} to {id2}, using overall avg")
                        yield all_avg
                        continue
                    l = int(avg[0])
                    if l > 400:
                        print(f"High latency from {id1} to {id2}: {l}ms")
                    yield l

        yield list(line())


latency_matrix = list(matrix())
with open("control/src/configs/latency_matrix.rs", "w") as f:
    f.write(f"pub const LATENCY_MATRIX: [[u32; {N}]; {N}] = ")
    f.write(str(latency_matrix))
    f.write(";")

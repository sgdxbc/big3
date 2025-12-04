num_faulty_nodes = 33
num_nodes = 3 * num_faulty_nodes + 1
num_shards = 1000

def nodes_of_shard(shard):
    # i = shard % num_nodes
    i = shard % (num_nodes - num_faulty_nodes)
    for _ in range(7):
        yield i
        i = (i + num_nodes // 3) % num_nodes


def pushing_nodes_of_shard(shard):
    for i in nodes_of_shard(shard):
        if i < num_nodes - num_faulty_nodes:
            return i
    assert False, "no pushing node found"


node_pushes = [0 for _ in range(num_nodes)]
for shard in range(num_shards):
    pusher = pushing_nodes_of_shard(shard)
    node_pushes[pusher] += 1

node_pushes = sorted([(i, c) for i, c in enumerate(node_pushes)], key=lambda x: x[1])
for i, c in node_pushes:
    if i >= num_nodes - num_faulty_nodes:
        assert c == 0
    else:
        print(i, c)
print()


node_stores = [0 for _ in range(num_nodes)]
for shard in range(num_shards):
    for i in nodes_of_shard(shard):
        node_stores[i] += 1

node_stores = sorted([(i, c) for i, c in enumerate(node_stores)], key=lambda x: x[1])
for i, c in node_stores:
    if i >= num_nodes - num_faulty_nodes:
        continue
    print(i, c)
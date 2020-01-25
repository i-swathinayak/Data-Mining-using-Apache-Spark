from pyspark import SparkContext
import time
import queue
import collections
from itertools import combinations
import sys


def graph_traversal_bfs(node):
    que = queue.Queue()
    que.put(node)
    # level[int(node)] = 0
    # marked[int(node)] = True
    marked = set()
    level_dict = {}
    level_dict[node] = 0
    marked.add(node)

    while not que.empty():
        x = que.get()
        for b in adjacency_dict[x]:
            # if not marked[int(b)]:
            if b not in marked:
                que.put(b)
                level_dict[b] = level_dict[x] + 1
                # level[int(b)] = level[int(x)] + 1
                marked.add(b)

    height=max(level_dict.values()) + 1
    # print(height)
    result = [[] for i in range(height)]
    for node, level in level_dict.items():
        result[level].append(node)
    # for i in range(1, nodes_count+1):
    #     if level[i] is not -1:
    #         result[level[i]].append(str(i))
    # print(result)
    return result


def compute_node_labels(root, bfs_result):
    height = len(bfs_result)
    shortest_paths_count = dict()

    shortest_paths_count[root] = 1

    if height > 1:
        for node in bfs_result[1]:
            shortest_paths_count[node] = 1

        for level in range(2, height):
            children_nodes = bfs_result[level]
            parent_nodes = bfs_result[level-1]
            for node in children_nodes:
                parents = set(adjacency_dict[node]).intersection(set(parent_nodes))
                paths = 0
                for parent in parents:
                    paths += shortest_paths_count[parent]
                shortest_paths_count[node] = paths

    return shortest_paths_count


# def compute_edge_credits(bfs_result, node_labels):
#     height = len(bfs_result)
#     edge_credits = dict()
#
#     for i in range(height-1, 0, -1):
#         for node in bfs_result[i]:
#             if i == (height-1):
#                 node_value = 1
#             else:
#                 children_nodes = set(adjacency_dict[node]).intersection(set(bfs_result[i + 1]))
#                 credit = 0.0
#                 for child in children_nodes:
#                     if node > child:
#                         credit += edge_credits[(child, node)]
#                     else:
#                         credit += edge_credits[(node, child)]
#                 node_value = 1 + credit
#             parent_nodes = set(adjacency_dict[node]).intersection(set(bfs_result[i - 1]))
#             credit = 0.0
#             for parent in parent_nodes:
#                 credit += node_labels[parent]
#             for parent in parent_nodes:
#                 edge_value = (float(node_labels[parent]) / float(credit)) * float(node_value)
#                 if parent < node:
#                     edge_credits[(parent, node)] = edge_value
#                 else:
#                     edge_credits[(node, parent)] = edge_value
#     return edge_credits


def compute_partial_betweenness(node):
    bfs_result = graph_traversal_bfs(node)
    shortest_paths = compute_node_labels(node, bfs_result)
    # edge_credits = compute_edge_credits(bfs_result, node_lables)

    height = len(bfs_result)
    edge_credits = dict()

    for i in range(height - 1, 0, -1):
        for node in bfs_result[i]:
            if i == (height - 1):
                node_value = 1
            else:
                children_nodes = set(adjacency_dict[node]).intersection(set(bfs_result[i + 1]))
                credit = 0.0
                for child in children_nodes:
                    if node > child:
                        credit += edge_credits[(child, node)]
                    else:
                        credit += edge_credits[(node, child)]
                node_value = 1 + credit
            parent_nodes = set(adjacency_dict[node]).intersection(set(bfs_result[i - 1]))
            credit = 0.0
            for parent in parent_nodes:
                credit += shortest_paths[parent]
            for parent in parent_nodes:
                edge_value = (float(shortest_paths[parent]) / float(credit)) * float(node_value)
                if parent < node:
                    edge_credits[(parent, node)] = edge_value
                    yield ((parent, node), edge_value)
                else:
                    edge_credits[(node, parent)] = edge_value
                    yield ((node, parent), edge_value)


def write_to_file(result_set, count):
    data = str(result_set[0])[1:-1]
    file.write(data)
    for k in range(1, count):
        file.write("\n")
        data = str(result_set[k])[1:-1]
        file.write(data)


# def compute_modularity(communities, edges_count, adjacency_dict, degree_dict):
#     sum = 0.0
#     denom_factor = 1.0 / float(2 * edges_count)
#     for community in communities:
#         node_combinations = combinations(community, 2)
#         for (node_1, node_2) in node_combinations:
#             aij = 1 if node_2 in adjacency_dict[node_1] else 0
#             ki = degree_dict[node_1]
#             kj = degree_dict[node_2]
#             sum += (aij - (float(ki*kj*denom_factor)))
#     return sum * denom_factor


def compute_constants(adjacency_dict, adjacency_keys, degree_dict, edges_count):
    constants_dict = {}
    denom_factor = 1.0 / float(2 * edges_count)
    for node_1 in adjacency_keys:
        for node_2 in adjacency_keys:
            aij = 1 if node_2 in adjacency_dict[node_1] else 0
            ki = degree_dict[node_1]
            kj = degree_dict[node_2]
            constants_dict[tuple(sorted([node_1, node_2]))] = aij - (float(ki * kj * denom_factor))
    return constants_dict


def compute_modularity(communities, edges_count, adjacency_dict, degree_dict):
    sum = 0.0
    denom_factor = 1.0 / float(2 * edges_count)
    for community in communities:
        for node_1 in community:
            for node_2 in community:
                sum += constants_dict.get(tuple(sorted([node_1, node_2])))
    return sum * denom_factor


def graph_traversal_dfs(temp, node, visited, adjacency_dict):
    visited.add(node)
    temp.append(node)
    for i in adjacency_dict[node]:
        if i not in visited:
            temp = graph_traversal_dfs(temp, i, visited, adjacency_dict)
    return temp


def compute_communities(adjacency_dict):
    visited = set()
    result = []
    for node in adjacency_dict:
        if node not in visited:
            temp = []
            result.append(graph_traversal_dfs(temp, node, visited, adjacency_dict))
    return result


if __name__ == '__main__':
    start_time = time.time()

    power_input = sys.argv[1]
    output_file_1 = sys.argv[2]
    output_file_2 = sys.argv[3]
    #
    # power_input = "/Users/swathinayak/PycharmProjects/DataMining-HW4/data/power_input.txt"
    # output_file_1 = "/Users/swathinayak/PycharmProjects/DataMining-HW4/data/swathi_nayak_task2_edge_betweenness_python.txt"
    # output_file_2 = "/Users/swathinayak/PycharmProjects/DataMining-HW4/data/swathi_nayak_task2_community_python.txt"

    sc = SparkContext.getOrCreate()

    train_data = sc.textFile(power_input)
    adjacency_rdd_1 = train_data.map(lambda line: line.split(" ")).map(lambda line: (line[0], line[1])).groupByKey().mapValues(set)
    adjacency_rdd_2 = train_data.map(lambda line: line.split(" ")).map(lambda line: (line[1], line[0])).groupByKey().mapValues(set)
    adjacency_rdd = adjacency_rdd_1.union(adjacency_rdd_2).reduceByKey(lambda x,y: x.union(y))
    node_rdd = adjacency_rdd.keys().distinct().map(lambda x: (x,))

    adjacency_dict = dict(adjacency_rdd.collect())
    nodes_count = len(adjacency_dict)

    edges_count=0
    for value in adjacency_dict.values():
        edges_count += len(value)
    edges_count = edges_count // 2

    edge_betweenness = node_rdd.flatMap(lambda node: compute_partial_betweenness(node[0])).reduceByKey(lambda x,y: x+y).map(lambda v: (v[0], v[1]/2)).sortBy(lambda x: x[1], ascending=False).collect()
    count = len(edge_betweenness)

    file = open(output_file_1, "w")
    write_to_file (edge_betweenness, count)

    print(time.time() - start_time)

    #   community detection
    degree_dict = {}
    for key, value in adjacency_dict.items():
        degree_dict[key] = len(value)

    adjacency_keys = list(adjacency_dict.keys())
    constants_dict = compute_constants(adjacency_dict, adjacency_keys, degree_dict, edges_count)
    communities_count = 1
    communities_a = list()
    communities_a.append(adjacency_keys)
    max_modularity = compute_modularity(communities_a, edges_count, adjacency_dict, degree_dict)
    max_communities = communities_a
    old_communities_size = len(communities_a)
    max_communities_size = 1

    # print(max_modularity)
    #
    while old_communities_size < nodes_count:
        max_edge = node_rdd.flatMap(lambda node: compute_partial_betweenness(node[0])).reduceByKey(lambda x, y: x + y).map(lambda v: (v[0], v[1] / 2)).sortBy(lambda x: x[1], ascending=False).take(1)
        node_a = max_edge[0][0][0]
        node_b = max_edge[0][0][1]
        adjacency_dict[node_a].remove(node_b)
        adjacency_dict[node_b].remove(node_a)
        communities = compute_communities(adjacency_dict)
        current_community_size = len(communities)

        if current_community_size != old_communities_size:
            current_modularity = compute_modularity(communities, edges_count, adjacency_dict, degree_dict)
            # print(current_modularity)

            if current_modularity > max_modularity:
                max_modularity = current_modularity
                max_communities_size = current_community_size
                max_communities = communities

            if current_community_size == nodes_count:
                break
            old_communities_size = current_community_size
        # print(max_modularity)
    #
    # print(max_modularity)
    # print(max_communities_size)
    # print(max_communities)

    final_communities = list()
    for community in max_communities:
        community = sorted(community)
        final_communities.append(community)
    final_result = sorted(final_communities, key=lambda x: (len(x), x))

    with open(output_file_2, 'w+') as out:
        for community in final_result:
            line_str = ''
            for node in community:
                line_str += "\'" + node + "\'"
                line_str += ', '
            line_str = line_str[:-2]
            out.write(line_str)
            out.write('\n')

    print(time.time() - start_time)










import argparse
import csv
from collections import deque, defaultdict

class Graph:
    def __init__(self):
        self.nodes = set()
        self.edges = defaultdict(list)

    def add_node(self, value):
        self.nodes.add(value)

    def add_edge(self, from_node, to_node):
        self.edges[from_node].append(to_node)
        self.edges[to_node].append(from_node)

def shortest_path_length(graph, node1, node2):
    if node1 == node2:
        return 0
    if (node1 not in graph.nodes) or \
       (node2 not in graph.nodes):
       return -1
    queue = deque()
    visited = defaultdict()
    queue.appendleft(node1)
    visited[node1] = 0
    while len(queue) > 0:
        current = queue.pop()
        for i in graph.edges[current]:
            if i in visited:
                continue
            visited[i] = visited[current]+1
            if i == node2:
                return visited[i]
            queue.appendleft(i)
    return -1


class Verifier:
    def __init__(self, outputRepo):
        self.outputRepo = outputRepo
        self.graph = Graph()

    def pre_process(self, batchFile):
        with open(batchFile, 'r') as csvfile:
            reader = csv.DictReader(csvfile, skipinitialspace=True)
            for row in reader:
                node1 = row['id1']
                node2 = row['id2']
                self.graph.add_node(node1)
                self.graph.add_node(node2)
                self.graph.add_edge(node1, node2)

    def process(self, streamFile):
        with open(streamFile, 'r') as csvfile,\
             open(self.outputRepo + '/output1.txt', 'w') as out1,\
             open(self.outputRepo + '/output2.txt', 'w') as out2,\
             open(self.outputRepo + '/output3.txt', 'w') as out3:
            reader = csv.DictReader(csvfile, skipinitialspace=True)
            for row in reader:
                node1 = row['id1']
                node2 = row['id2']
                distance = shortest_path_length(self.graph, node1, node2)
                if distance == 1:
                    out1.write('trusted\n')
                    out2.write('trusted\n')
                    out3.write('trusted\n')
                elif distance == 2:
                    out1.write('unverified\n')
                    out2.write('trusted\n')
                    out3.write('trusted\n')
                elif distance < 5 and distance > 2:
                    out1.write('unverified\n')
                    out2.write('unverified\n')
                    out3.write('trusted\n')
                else:
                    out1.write('unverified\n')
                    out2.write('unverified\n')
                    out3.write('unverified\n')


def main(batchFile, streamFile, outputRepo):
    verifier = Verifier(outputRepo)
    verifier.pre_process(batchFile)
    verifier.process(streamFile)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', '-b',
        metavar='BATCH_FILE', help='batch input file', required=True)
    parser.add_argument('--stream', '-s',
        metavar='STREAM_FILE', help='stream input file', required=True)
    parser.add_argument('--output', '-o',
        metavar='OUTPUT_REPO', help='repo for output files', default='.')
    args = parser.parse_args()
    main(args.batch, args.stream, args.output)
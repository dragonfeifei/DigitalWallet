import argparse
import csv
import networkx as nx

class Verifier:
    def __init__(self, outputRepo):
        self.outputRepo = outputRepo
        self.graph = nx.Graph()

    def pre_process(self, batchFile):
        with open(batchFile, 'r') as csvfile:
            reader = csv.DictReader(csvfile, skipinitialspace=True)
            for row in reader:
                node1 = row['id1']
                node2 = row['id2']
                self.graph.add_nodes_from([node1, node2])
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
                if node1 in self.graph and \
                   node2 in self.graph and \
                   nx.has_path(self.graph, node1, node2):
                    distance = nx.shortest_path_length(self.graph, node1, node2)
                    if distance == 1:
                        out1.write('trusted\n')
                        out2.write('trusted\n')
                        out3.write('trusted\n')
                        continue
                    elif distance == 2:
                        out1.write('unverified\n')
                        out2.write('trusted\n')
                        out3.write('trusted\n')
                        continue
                    elif distance < 5:
                        out1.write('unverified\n')
                        out2.write('unverified\n')
                        out3.write('trusted\n')
                        continue
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
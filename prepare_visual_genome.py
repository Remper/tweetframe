# Extracts objects and relations and normalises them into wordnet offsets

import json
import argparse
import gzip
from os import path
from nltk.corpus import wordnet as wn


class Relation:
    def __init__(self, predicate, synset):
        self.predicate = predicate

        self.synset = synset

        self.objects = set()
        self.subjects = set()


def process_synset(object, synsets):
    obj_synsets = object['synsets']
    # if len(obj_synsets) > 1:
        # print("Ambiguous synset: [%s]. Using only first" % (", ".join(obj_synsets)))

    if len(obj_synsets) == 0:
        return None

    synset = wn.synset(obj_synsets[0])
    resolved_synset = str(synset.offset()).zfill(8) + '-' + synset.pos()

    if resolved_synset not in synsets:
        synsets[resolved_synset] = obj_synsets[0]

    return resolved_synset


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process raw relationships.json file and output synsets used')
    parser.add_argument('--input', required=True, help='Work directory', metavar='#')
    args = parser.parse_args()

    relations = dict()
    synsets = dict()
    with open(path.join(args.input, "raw_relations.tsv"), 'wt') as writer:
        first = True
        with gzip.open(path.join(args.input, "relationships.json.gz"), 'rt') as reader:
            images = json.load(reader)
            for image in images:
                for relationship in image['relationships']:
                    rel_synset = process_synset(relationship, synsets)
                    obj_synset = process_synset(relationship['object'], synsets)
                    subj_synset = process_synset(relationship['subject'], synsets)
                    if rel_synset == "02488834-v" and obj_synset == "07628870-n" and subj_synset == "03650173-n":
                        print(json.dumps(relationship))

                    if rel_synset is None:
                        continue

                    if rel_synset not in relations:
                        relations[rel_synset] = Relation(relationship['predicate'], rel_synset)
                    cur_relation = relations[rel_synset]

                    if obj_synset is not None:
                        cur_relation.objects.add(obj_synset)
                    if subj_synset is not None:
                        cur_relation.subjects.add(subj_synset)

                    if obj_synset is None or subj_synset is None:
                        continue

                    if not first:
                        writer.write('\n')
                    first = False
                    writer.write(rel_synset)
                    writer.write('\t')
                    writer.write(obj_synset)
                    writer.write('\t')
                    writer.write(subj_synset)

    with open(path.join(args.input, "synsets.tsv"), 'w') as writer:
        first = True
        for synset in synsets:
            if not first:
                writer.write('\n')
            first = False
            writer.write(synset)
            writer.write('\t')
            writer.write(synsets[synset])

    with open(path.join(args.input, "relations.tsv"), 'wt') as writer:
        first = True
        for id in relations:
            relation = relations[id]
            if not first:
                writer.write('\n')
            first = False
            writer.write(relation.predicate)
            writer.write('\t')
            writer.write(relation.synset)
            writer.write('\t')
            writer.write(",".join(relation.objects))
            writer.write('\t')
            writer.write(",".join(relation.subjects))

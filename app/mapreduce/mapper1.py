#!/usr/bin/env python3
import sys, re, os
from collections import defaultdict

def tokenize(text):
    text = text.lower()
    text = re.sub(r'http\S+|www\S+', '', text)
    text = re.sub(r'\S+@\S+', '', text)
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return [t for t in text.split() if t]

def main():
    doc_id = os.path.basename(os.environ.get('mapreduce_map_input_file', 'doc_unknown')).replace('.txt', '').replace('.', '_')
    doc_terms = defaultdict(int)
    doc_token_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        for token in tokenize(line):
            doc_terms[token] += 1
            doc_token_count += 1
    
    for term, count in doc_terms.items():
        print(f'{term}\t{doc_id}:{count}')
    
    print(f'__DOC_STAT__:{doc_id}\t{doc_id},{len(doc_terms)},{doc_token_count}')

if __name__ == '__main__':
    main()
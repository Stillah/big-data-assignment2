#!/usr/bin/env python3
import sys
from collections import defaultdict

def emit_index_entry(term, doc_freq_map):
    postings = ','.join([f'{doc_id}:{freq}' for doc_id, freq in sorted(doc_freq_map.items())])
    print(f'{term}\t{postings}')

def main():
    current_key = None
    doc_freq_map = {}
    doc_stats = {}
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        
        key, value = parts
        
        if key.startswith('__DOC_STAT__'):
            doc_id = key.split(':')[1]
            stat_parts = value.split(',')
            if len(stat_parts) == 3:
                doc_stats[doc_id] = (int(stat_parts[1]), int(stat_parts[2]))
            continue
        
        if current_key is not None and current_key != key:
            emit_index_entry(current_key, doc_freq_map)
            doc_freq_map = {}
        
        current_key = key
        
        for entry in value.split(','):
            if ':' in entry:
                doc_id, count = entry.split(':')
                doc_freq_map[doc_id] = doc_freq_map.get(doc_id, 0) + int(count)
    
    if current_key is not None:
        emit_index_entry(current_key, doc_freq_map)
    
    for doc_id, (unique_terms, token_count) in doc_stats.items():
        print(f'__DOC_STATS__\t{doc_id},{unique_terms},{token_count}')
    
    for term in sorted(doc_freq_map.keys()):
        print(f'__VOCABULARY__\t{term}')

if __name__ == '__main__':
    main()
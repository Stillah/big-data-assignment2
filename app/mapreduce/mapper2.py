#!/usr/bin/env python3
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        
        key, value = parts
        
        if key.startswith('__DOC_STATS__') or key.startswith('__VOCABULARY__'):
            print(line)
        else:
            postings_str = ','.join(value.split(','))
            print(f'{key}\tDOC_FREQ:{len(value.split(","))}\tPOSTINGS:{postings_str}')

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import sys, math
from collections import defaultdict

def main():
    total_documents = 0
    doc_stats = {}
    term_postings = {}
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        
        key = parts[0]
        
        if key.startswith('__DOC_STATS__'):
            stat_parts = parts[1].split(',')
            if len(stat_parts) == 3:
                doc_stats[stat_parts[0]] = (int(stat_parts[1]), int(stat_parts[2]))
                total_documents += 1
        elif not key.startswith('__VOCABULARY__'):
            doc_freq = None
            postings = None
            for part in parts[1:]:
                if part.startswith('DOC_FREQ:'):
                    doc_freq = int(part.split(':')[1])
                elif part.startswith('POSTINGS:'):
                    postings = part.split(':', 1)[1]
            if doc_freq and postings:
                term_postings[key] = (doc_freq, postings)
    
    idf_scores = {term: math.log(total_documents / (df + 1)) + 1 for term, (df, _) in term_postings.items() if total_documents > 0}
    
    for term, (doc_freq, postings) in sorted(term_postings.items()):
        idf = idf_scores.get(term, 0.0)
        postings_with_tfidf = []
        for entry in postings.split(','):
            if ':' in entry:
                doc_id, term_freq = entry.split(':')
                tfidf = int(term_freq) * idf
                postings_with_tfidf.append(f'{doc_id}:{term_freq}:{tfidf:.4f}')
        print(f'{term}\t{doc_freq},{idf:.4f},{",".join(postings_with_tfidf)}')
    
    print(f'__COLLECTION_STATS__\ttotal_docs:{total_documents}')
    for doc_id, (unique_terms, token_count) in sorted(doc_stats.items()):
        avg_term_count = token_count / max(unique_terms, 1)
        print(f'__DOC_STATS__\t{doc_id},{unique_terms},{token_count},{avg_term_count:.4f}')
    
    for i, term in enumerate(sorted(term_postings.keys())):
        print(f'__VOCABULARY__\t{i},{term}')

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import sys
import argparse
import logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndexUpdater:
    """Incrementally updates Cassandra index tables."""
    
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        
        self.cluster = Cluster(contact_points=[host],
                                port=port,
                                connect_timeout=10)
        self.session = self.cluster.connect(keyspace)
        logger.info(f"Connected to Cassandra at {host}:{port}. Keyspace: {keyspace}")
    
    def close(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
    
    def get_max_term_id(self):
        """Get the maximum term_id from vocabulary table."""
        try:
            result = self.session.execute(
                "SELECT MAX(term_id) as max_id FROM vocabulary"
            )
            max_id = result[0].max_id
            return max_id if max_id is not None else 0
        except Exception as e:
            logger.warning(f"Could not get max term_id: {e}, starting from 0")
            return 0
    
    def parse_index_file(self, index_path):
        vocab = {}
        postings = {}
        doc_stats = {}
        collection_stats = {}
        
        try:
            with open(index_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse document statistics
                    if line.startswith("__DOC_STATS__"):
                        parts = line.split('\t')
                        if len(parts) >= 5:
                            doc_id = parts[1]
                            unique_terms = int(parts[2])
                            token_count = int(parts[3])
                            avg_term_freq = float(parts[4])
                            doc_stats[doc_id] = (unique_terms, token_count, avg_term_freq)
                    
                    # Parse collection statistics
                    elif line.startswith("__COLLECTION_STATS__"):
                        parts = line.split('\t')
                        if len(parts) >= 3:
                            stat_name = parts[1]
                            stat_value = parts[2]
                            collection_stats[stat_name] = stat_value
                    
                    # Parse vocabulary and postings
                    elif not line.startswith("__"):
                        parts = line.split('\t')
                        if len(parts) >= 3:
                            term = parts[0]
                            doc_freq_idf = parts[1].split(',')
                            
                            if len(doc_freq_idf) >= 2:
                                doc_freq = int(doc_freq_idf[0])
                                idf = float(doc_freq_idf[1])
                                
                                vocab[term] = (idf, doc_freq)
                                
                                # Parse postings (doc:freq:tfidf,...)
                                if len(parts) > 2:
                                    postings_str = parts[2]
                                    postings_list = []
                                    
                                    for posting in postings_str.split(','):
                                        posting = posting.strip()
                                        if ':' in posting:
                                            posting_parts = posting.split(':')
                                            if len(posting_parts) >= 3:
                                                doc_id = posting_parts[0]
                                                term_freq = int(posting_parts[1])
                                                tfidf = float(posting_parts[2])
                                                postings_list.append((doc_id, term_freq, tfidf))
                                    
                                    if postings_list:
                                        postings[term] = postings_list
            
            logger.info(f"Parsed index: {len(vocab)} terms, {len(doc_stats)} documents")
            return vocab, postings, doc_stats, collection_stats
        
        except Exception as e:
            logger.error(f"Error parsing index file: {e}")
            return {}, {}, {}, {}
    
    def update_vocabulary(self, vocab):
        """Assigns new term_ids to new terms and updates doc_frequency."""
        logger.info("Updating vocabulary table")
        
        max_term_id = self.get_max_term_id()
        next_term_id = max_term_id + 1
        
        # Get existing terms
        existing_terms = set()
        try:
            results = self.session.execute("SELECT term FROM vocabulary")
            existing_terms = {row.term for row in results}
        except:
            pass
        
        inserted = 0
        updated = 0
        
        for term, (idf, doc_freq) in vocab.items():
            if term in existing_terms:
                # Update existing term's doc_frequency and idf
                query = f"""
                    UPDATE vocabulary SET doc_frequency = ?, idf = ?
                    WHERE term = ?
                """
                self.session.execute(query, (doc_freq, idf, term))
                updated += 1
            else:
                # Insert new term with new term_id
                query = f"""
                    INSERT INTO vocabulary (term_id, term, idf, doc_frequency)
                    VALUES (?, ?, ?, ?)
                """
                self.session.execute(query, (next_term_id, term, idf, doc_freq))
                next_term_id += 1
                inserted += 1
        
        logger.info(f"Vocabulary updated: {inserted} inserted, {updated} updated")
    
    def update_inverted_index(self, postings):
        logger.info("Updating inverted index table")
        
        inserted = 0
        
        for term, posting_list in postings.items():
            for doc_id, term_freq, tfidf in posting_list:
                query = f"""
                    INSERT INTO inverted_index (term, doc_id, term_freq, tfidf)
                    VALUES (?, ?, ?, ?)
                """
                self.session.execute(query, (term, doc_id, term_freq, tfidf))
                inserted += 1
        
        logger.info(f"Inverted index updated: {inserted} postings inserted")
    
    def update_document_stats(self, doc_stats):
        logger.info("Updating document statistics table")
        
        inserted = 0
        
        for doc_id, (unique_terms, token_count, avg_term_freq) in doc_stats.items():
            query = f"""
                INSERT INTO document_stats (doc_id, unique_terms, token_count, avg_term_freq)
                VALUES (?, ?, ?, ?)
            """
            self.session.execute(query, (doc_id, unique_terms, token_count, avg_term_freq))
            inserted += 1
        
        logger.info(f"Document statistics updated: {inserted} documents inserted")
    
    def update_collection_stats(self, collection_stats):
        """
        Update collection-level statistics.
        """
        logger.info("Updating collection statistics table")
        
        updated = 0
        
        for stat_name, stat_value in collection_stats.items():
            query = f"""
                INSERT INTO collection_stats (stat_name, stat_value)
                VALUES (?, ?)
            """
            self.session.execute(query, (stat_name, stat_value))
            updated += 1
        
        logger.info(f"Collection statistics updated: {updated} statistics")
    
    def update_index(self, index_path):
        """
        Main update operation: parse index and update all the tables
        """
        logger.info(f"Starting index update from {index_path}")
        
        # Parse the index file
        vocab, postings, doc_stats, collection_stats = self.parse_index_file(index_path)
        
        if not vocab and not postings and not doc_stats:
            logger.warning("No data found in index file")
            return False
        
        # Update tables
        if vocab: self.update_vocabulary(vocab)
        if postings: self.update_inverted_index(postings)
        if doc_stats: self.update_document_stats(doc_stats)
        if collection_stats: self.update_collection_stats(collection_stats)
        
        logger.info("Index update completed successfully")
        return True


def main():
    parser = argparse.ArgumentParser(description="Update Cassandra index tables with new document data")
    parser.add_argument(
        "--index-path",
        required=True,
        help="Path to index data file to load"
    )
    parser.add_argument(
        "--cassandra-host",
        default="localhost",
        help="ScyllaDB host (default: localhost)"
    )
    parser.add_argument(
        "--cassandra-port",
        type=int,
        default=9042,
        help="ScyllaDB port (default: 9042)"
    )
    parser.add_argument(
        "--keyspace",
        default="search_index",
        help="Cassandra keyspace (default: search_index)"
    )
    
    args = parser.parse_args()
    
    updater = None
    try:
        updater = IndexUpdater(args.cassandra_host, args.cassandra_port, args.keyspace)
        success = updater.update_index(args.index_path)
        
        if not success: sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    finally:
        if updater:
            updater.close()


if __name__ == "__main__":
    main()

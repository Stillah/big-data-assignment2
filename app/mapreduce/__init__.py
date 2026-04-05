"""
MapReduce Indexing Module
Provides Hadoop MapReduce pipelines for document indexing with BM25 scoring support.

Components:
- mapper1.py: First stage mapper (tokenization and term extraction)
- reducer1.py: First stage reducer (index construction)
- mapper2.py: Second stage mapper (statistics calculation)
- reducer2.py: Second stage reducer (IDF and final index)
- orchestrator.py: Job orchestrator for submitting pipelines to Hadoop
- index_reader.py: Utilities for reading and querying the index
"""

try:
    from .orchestrator import MapReduceOrchestrator
    from .index_reader import IndexReader
except ImportError:
    pass

__all__ = ['MapReduceOrchestrator', 'IndexReader']

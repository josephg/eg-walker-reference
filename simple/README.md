# Simple Fully Replayable History implementation

This directory contains a simple implementation of a FRH collaborative editing
object for sequences, built on top of Fugue.

The implementation is complete, but missing the 'standard' optimizations which
would be advised for a real-world application.

The code is split between two files:

- **[causal-graph.ts](src/causal-graph.ts)** contains a simple data structure which stores:
  - Operation IDs (agent, sequence number pairs) for each operation. Internally, operations are identified by a simple incrementing integer for each stored op. This part of the data structure maps from these integers to the full (agent, seq) pairs.
  - The graph *parents* for each operation. This information encodes the actual graph structure - so, when each operation happened relative to other operations.
- **[index.ts](src/index.ts)** contains everything else:
  - The *SimpleOpLog* data structure, which contains the operations themselves. In a full implementation, it is this data structure that would be stored and merged with remote peers.
  - Code to replay the oplog from scratch, and calculate the document state (a snapshot) at any version.
  - And there's also code to update a document snapshot to merge in more recent changes.

The upcoming paper will have a lot more details about how and why it works like this.

# Replayable Event Graph (reference implementation)

This is a simple reference implementation of a Replayable Event Graph: A new approach to collaborative editing.

This codebase contains a sequence-REG implementation based around the FUGUE sequence CRDT.

Conceptually, rather than storing a list of transformed operations (OT), or storing a list of intermediate ordered items (CRDTs), regs store an append-only, immutable list of *original operations*. This is the list of edits *as they actually happened*, including causal information (*when* each operation happened relative to all other operations.)

In practice, this means operations look like this:

```typescript
interface ListOperation<T> {
  parentVersions: VersionId[],
  id: VersionId, // Globally unique.

  type: 'insert' | 'delete',
  position: number,
  insertedContent?: T, // Only needed for inserts
}
```

Unlike OT based systems, operations are never modified once they are created.

Unlike CRDT based systems, the operations are stored in this original form, regardless of the sequencing algorithm used. (The same persistent data structure and wire format is used regardless of if we're ordering items with RGA or FUGUE or any other approach). The set of operations is essentially a simple grow-only set CRDT that we turn into a document state by processing the operations independently on every peer.

The upcoming paper will have a lot more detail about this algorithm. Stay tuned.

This algorithm was first created in the [diamond types](https://github.com/josephg/diamond-types) library. This implementation is designed to be fully compatible with diamond types, and tested as such. (See included conformance tests).

## Usage example

```javascript
import * as reg from 'reference-reg'

const oplog1 = reg.createOpLog()

// Insert 'h', 'i' from user1.
reg.localInsert(oplog1, 'user1', 0, 'h', 'i')
console.log(reg.checkoutSimpleString(oplog1)) // 'hi'

// Users 1 and 2 concurrently insert A and B at the start of the document
const oplog2 = reg.createOpLog() // In a new document

const v = reg.getLatestVersion(oplog2) // [] in this case.
reg.pushOp(oplog2, ['user1', 0], v, 'ins', 0, 'A')
reg.pushOp(oplog2, ['user2', 0], v, 'ins', 0, 'B')

// Prints 'AB' - since fugue tie breaks by ordering by agent ID.
console.log(reg.checkoutSimpleString(oplog2))

// Now lets simulate the same thing using 2 oplogs.
const oplogA = reg.createOpLog()
reg.localInsert(oplogA, 'user1', 0, 'A')

const oplogB = reg.createOpLog()
reg.localInsert(oplogB, 'user2', 0, 'B')

// The two users sync changes:
reg.mergeOplogInto(oplogA, oplogB)
reg.mergeOplogInto(oplogB, oplogA)

// And now they both see AB.
console.log(reg.checkoutSimpleString(oplogA)) // Also AB.
console.log(reg.checkoutSimpleString(oplogB)) // Also AB.

// Finally lets make a branch and update it.
const branch = reg.createEmptyBranch()
reg.mergeChangesIntoBranch(branch, oplogA)
console.log(branch.snapshot) // ['A', 'B'].
```



## Reference implementation

This directory contains a simple implementation of a reg collaborative editing
object for sequences, built on top of Fugue.

The code is split between two files:

- **[causal-graph.ts](src/causal-graph.ts)** contains a simple data structure which stores:
  - Operation IDs (agent, sequence number pairs) for each operation. Internally, operations are identified by a simple incrementing integer for each stored op. This part of the data structure maps from these integers to the full (agent, seq) pairs.
  - The graph *parents* for each operation. This information encodes the actual graph structure - so, when each operation happened relative to other operations.
- **[index.ts](src/index.ts)** contains everything else:
  - The *SimpleOpLog* data structure, which contains an array storing the operations themselves. It is this data structure that gets merged with remote peers.
  - Code to replay the oplog from scratch, and calculate the document state (a snapshot) at any version.
  - And there's also code to update a document snapshot to merge in more recent changes.

The implementation is complete, but missing the 'standard' optimizations which
would be advised for a real-world application. Based on early tests, it runs about 200x slower than the implementation in diamond-types. (But its also about 30x fewer lines of code.)

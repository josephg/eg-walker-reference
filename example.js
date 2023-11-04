import * as frh from './dist/src/index.js'

const oplog1 = frh.createOpLog()

// Insert 'h', 'i' from user1.
frh.localInsert(oplog1, 'user1', 0, 'h', 'i')
console.log(frh.checkoutSimpleString(oplog1)) // 'hi'

// Users 1 and 2 concurrently insert A and B at the start of the document
const oplog2 = frh.createOpLog() // In a new document

const v = frh.getLatestVersion(oplog2) // [] in this case.
frh.pushOp(oplog2, ['user1', 0], v, 'ins', 0, 'A')
frh.pushOp(oplog2, ['user2', 0], v, 'ins', 0, 'B')

// Prints 'AB' - since fugue tie breaks by ordering by agent ID.
console.log(frh.checkoutSimpleString(oplog2))

// Now lets simulate the same thing using 2 oplogs.
const oplogA = frh.createOpLog()
frh.localInsert(oplogA, 'user1', 0, 'A')

const oplogB = frh.createOpLog()
frh.localInsert(oplogB, 'user2', 0, 'B')

// The two users sync changes:
frh.mergeOplogInto(oplogA, oplogB)
frh.mergeOplogInto(oplogB, oplogA)

// And now they both see AB.
console.log(frh.checkoutSimpleString(oplogA)) // Also AB.
console.log(frh.checkoutSimpleString(oplogB)) // Also AB.

// Finally lets make a branch and update it.
const branch = frh.createEmptyBranch()
frh.mergeChangesIntoBranch(branch, oplogA)
console.log(branch.snapshot) // ['A', 'B'].
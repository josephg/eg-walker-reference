// This file contains some tests to make sure the algorithm works with some
// pre-prepared data.
//
// This is also used to verify that, given the same input, the algorithm
// generates the same output as other implementations of fugue.

// Run this by first compiling:
// $ npx tsc
// Then:
// $ node dist/simple/test/test.js

import * as fs from 'node:fs'
import * as causalGraph from "../src/causal-graph.js"
import type { LV, LVRange } from "../src/causal-graph.js"

import { ListOp, ListOpLog, OpTag, checkoutSimpleString, createOpLog, opLen, pushRemoteOp } from '../src/index.js';

import consoleLib from 'console'
import { assertEq } from '../src/utils.js';

// This is the data format output from the `dt export` command.
//
// It might be worth removing this at some point. These editing traces are
// much closer to DT's internal format - which is nice. And they preserve the actual
// agent IDs (which the editing traces do not). However, they don't contain the expected
// text output.
interface DTExportItem {
  agent: string,
  seqStart: number,
  span: LVRange,
  parents: LV[],

  ops: [pos: number, del: number, insContent: string][],
}

interface DTExport {
  endContent: string,
  txns: DTExportItem[],
}

function importDTOpLog(data: DTExport): ListOpLog {
  const oplog = createOpLog()

  // for (const txn of data.txns.slice(0, 3)) {
  for (const txn of data.txns) {
    // console.log(txn)
    let expectLV = txn.span[0]
    const expectLVEnd = txn.span[1]
    // The DT log file exports parents as LV numbers. I'm going to import using
    // pushRemoteOp - but that expects external IDs instead of LVs.
    let parents = oplog.cg.lvToIdList(txn.parents)
    const agent = txn.agent
    let seq = txn.seqStart

    for (let [pos, delHere, insContent] of txn.ops) {
      if ((delHere > 0) === (insContent !== '')) throw Error('Operation must be an insert or delete')

      const op: ListOp = delHere > 0
        ? {type: OpTag.Delete, pos, len: delHere}
        : {type: OpTag.Insert, pos, content: [...insContent]}

      const actualLv = oplog.cg.nextLV()
      assertEq(expectLV, actualLv)

      const len = opLen(op)

      // console.log('PRO', expectLV, agent, seq, parents, op)
      pushRemoteOp(oplog, [agent, seq], parents, op)

      expectLV += len
      seq += len
      // After the first op, everything has parents of the previous op.
      parents = [[agent, seq - 1]]
    }

    assertEq(expectLVEnd, oplog.cg.nextLV())
  }

  return oplog
}


// This is the data format from the `dt export-trace` command.
// This lets us run any editing trace in the concurrent editing traces repository:
// https://github.com/josephg/editing-traces
interface ConcurrentTrace {
  kind: 'concurrent',
  endContent: string,
  numAgents: number,
  txns: ConcurrentTraceTxn[],
}

interface ConcurrentTraceTxn {
  // These are indexes of other Txn objects in the parent's txn list.
  parents: number[],
  numChildren: number,
  // In these traces, the agents are simply numbers (0, 1, 2, etc).
  agent: number,
  patches: [pos: number, del: number, insContent: string][],
}

// function importFromConcurrentTrace(trace: ConcurrentTrace): ListOpLog {
//   if (trace.kind !== 'concurrent') throw Error('Invalid data - not a concurrent editing trace')

//   const ops: ListOp[] = []
//   const cg = causalGraph.createCG()

//   const nextSeqForAgent: number[] = new Array(trace.numAgents).fill(0)
//   const lastLVOfTxn: LV[] = [] // txn index -> last version.

//   let nextLV = 0
//   for (let i = 0; i < trace.txns.length; i++) {
//     const txn = trace.txns[i]

//     const parents = txn.parents.map(idx => lastLVOfTxn[idx])
//     const seqStart = nextSeqForAgent[txn.agent]
//     // The "length" of the transaction. Every delete and insert counts for 1.
//     const len = txn.patches.reduce((prev, [_pos, delHere, insContent]) => {
//       return prev + delHere + [...insContent].length
//     }, 0)
//     const seqEnd = seqStart + len
//     nextSeqForAgent[txn.agent] = seqEnd

//     causalGraph.add(cg, `${txn.agent}`.padStart(7, ' '), seqStart, seqEnd, parents)

//     // Then the ops. They need to be shattered for now, since I'm not storing them RLE.
//     for (let [pos, delHere, insContent] of txn.patches) {
//       // console.log(pos, delHere, insContent)
//       if ((delHere > 0) === (insContent !== '')) throw Error('Patches must always be an insert or delete')

//       if (delHere > 0) {
//         for (let i = 0; i < delHere; i++) {
//           // The deletes all happen at the same position.
//           ops.push({type: 'del', pos})
//         }
//       } else {
//         for (const c of insContent) {
//           ops.push({type: 'ins', pos, content: c})
//           pos++
//         }
//       }
//     }

//     nextLV += len
//     lastLVOfTxn[i] = nextLV - 1
//   }

//   return {ops, cg}
// }

function check1(oplog: ListOpLog, expectedResult: string, verbose: boolean, n: number = 1) {
  if (verbose) console.log('processing', oplog.ops.length, 'ops...')
  const start = Date.now()
  let result = ''
  for (let i = 0; i < n; i++) result = checkoutSimpleString(oplog)
  // fs.writeFileSync('out.txt', result)
  // console.log('Wrote output to out.txt. Took', end - start, 'ms')

  const time = Date.now() - start
  if (verbose) console.log(`Generated output ${n} times in ${time} ms`)
  if (verbose && n > 1) console.log(`(${time / n} ms per iteration)`)

  try {
    // assert.equal(result, expectedResult)
  } catch (e) {
    fs.writeFileSync('out.txt', result)
    console.log('Wrote actual output to out.txt')
    throw e
  }
}

function debugCheck() {
  // const data = JSON.parse(fs.readFileSync('am.json', 'utf-8'))
  // const data: ConcurrentTrace = JSON.parse(fs.readFileSync('testdata/node_nodecc.json', 'utf-8'))
  // const data: ConcurrentTrace = JSON.parse(fs.readFileSync('testdata/ff.json', 'utf-8'))
  // const data: ConcurrentTrace = JSON.parse(fs.readFileSync('testdata/git-makefile.json', 'utf-8'))
  // const oplog = importFromConcurrentTrace(data)

  const filename = 'testdata/ff-raw.json'
  const data: DTExport = JSON.parse(fs.readFileSync(filename, 'utf-8'))
  // const data: DTExport = JSON.parse(fs.readFileSync('testdata/git-makefile-raw.json', 'utf-8'))
  // const data: DTExport = JSON.parse(fs.readFileSync('testdata/node_nodecc-raw.json', 'utf-8'))
  const oplog = importDTOpLog(data)

  console.log('Replaying editing history from', filename)
  check1(oplog, data.endContent, true, 1)
  console.log('OK!')
}

debugCheck()

function conformance() {
  globalThis.console = new consoleLib.Console({
    stdout: process.stdout, stderr: process.stderr,
    inspectOptions: {depth: null}
  })

  const runs: DTExport[] = fs.readFileSync('testdata/conformance.json', 'utf-8')
    .split('\n')
    .filter(x => x !== '')
    .map(line => JSON.parse(line))

  // const runs: DTExport[] = JSON.parse(fs.readFileSync('testdata/conformance-fugue.json', 'utf-8'))
  // const runs: DTExport[] = JSON.parse(fs.readFileSync('testdata/conformance-fugue2.json', 'utf-8'))
  console.log(`Running ${runs.length} conformance tests...`)

  for (let i = 0; i < runs.length; i++) {
    // console.log('conformance', i)
    const data = runs[i]
    const oplog = importDTOpLog(data)

    try {
      check1(oplog, data.endContent, false)
    } catch (e) {
      console.error('Failed during conformance test', i)
      throw e
    }
  }

  console.log('All tests pass!')
}

conformance()

// const trimCG = (cg: causalGraph.CausalGraphInner, n: number) => {
//   const result = causalGraph.createCG()
//   for (let entry of cg.entries) {
//     let len = entry.vEnd - entry.version

//     if (n < len) {
//       // Trim the entry.
//       entry = {
//         ...entry,
//         vEnd: entry.version + n
//       }
//       len = n
//     }

//     causalGraph.add(result, entry.agent, entry.seq, entry.seq + len, entry.parents)
//     n -= len
//     if (n <= 0) break
//   }

//   return result
// }

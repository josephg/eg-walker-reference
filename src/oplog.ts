// An oplog is a collection of operations made by one or multiple users.
//
// This is intentionally not super optimized.

import * as fs from 'node:fs'
import * as causalGraph from "./causal-graph.js";
import { LV, LVRange, RawVersion } from "./types.js";
// import { assert } from './utils.js';
import { mergeString } from './merge.js';
import assert from 'node:assert/strict'
import { assertSorted } from './utils.js';

export const enum ListOpType {
  Ins = 0,
  Del = 1,
}

export interface ListOp<T = any> {
  type: ListOpType,
  pos: number,
  content?: T, // Required if the operation is an insert. Always a single item. (Eg 1 character).
}

export interface ListOpLog<T = any> {
  // The LV for each op is its index in this list.
  ops: ListOp<T>[],
  cg: causalGraph.CausalGraph,
}

export function createOpLog<T = any>(): ListOpLog<T> {
  return {
    ops: [], cg: causalGraph.createCG()
  }
}

export function localInsert<T>(oplog: ListOpLog<T>, agent: string, pos: number, content: T) {
  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+1, oplog.cg.heads)
  oplog.ops.push({ type: ListOpType.Ins, pos, content })
}

export function localDelete<T>(oplog: ListOpLog<T>, agent: string, pos: number, len: number = 1) {
  if (len === 0) throw Error('Invalid delete length')

  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+len, oplog.cg.heads)
  for (let i = 0; i < len; i++) {
    oplog.ops.push({ type: ListOpType.Del, pos })
  }
}



export function mergeOplogInto<T>(dest: ListOpLog<T>, src: ListOpLog<T>) {
  let vs = causalGraph.summarizeVersion(dest.cg)
  const [commonVersion, _remainder] = causalGraph.intersectWithSummary(src.cg, vs)
  // `remainder` lists items in dest that are not in src. Not relevant!

  // Now we need to get all the versions since commonVersion.
  const ranges = causalGraph.diff(src.cg, commonVersion, src.cg.heads).bOnly

  // Copy the missing CG entries.
  const cgDiff = causalGraph.serializeDiff(src.cg, ranges)
  causalGraph.mergePartialVersions(dest.cg, cgDiff)

  // And copy the corresponding oplog entries.
  for (const [start, end] of ranges) {
    for (let i = start; i < end; i++) {
      dest.ops.push(src.ops[i])
    }
  }
}




// This code is for testing the algorithm.

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

function importOpLogOld(data: DTExport): ListOpLog {
  const ops: ListOp[] = []
  const cg = causalGraph.createCG()

  // I'm going to reuse the LVs from diamond types directly.
  // But we need to shatter them.
  // const nextSeqForAgent: Record<string, number> = {}

  for (const txn of data.txns) {
    // const seqStart = nextSeqForAgent[txn.agent] ?? 0
    const len = txn.span[1] - txn.span[0]
    const seqStart = txn.seqStart
    const seqEnd = seqStart + len
    causalGraph.add(cg, txn.agent, seqStart, seqEnd, txn.parents)

    // nextSeqForAgent[txn.agent] = seqEnd

    // Then the ops. They need to be shattered for now, since I'm not storing them RLE.
    for (let [pos, delHere, insContent] of txn.ops) {
      // console.log(pos, delHere, insContent)
      if ((delHere > 0) === (insContent !== '')) throw Error('Operation must be an insert or delete')

      if (delHere > 0) {
        for (let i = 0; i < delHere; i++) {
          // The deletes all happen at the same position.
          ops.push({type: ListOpType.Del, pos})
        }
      } else {
        for (const c of insContent) {
          ops.push({type: ListOpType.Ins, pos, content: c})
          pos++
        }
      }
    }
  }

  return {ops, cg}
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

function importFromConcurrentTrace(trace: ConcurrentTrace): ListOpLog {
  if (trace.kind !== 'concurrent') throw Error('Invalid data - not a concurrent editing trace')

  const ops: ListOp[] = []
  const cg = causalGraph.createCG()

  const nextSeqForAgent: number[] = new Array(trace.numAgents).fill(0)
  const lastLVOfTxn: LV[] = [] // txn index -> last version.

  let nextLV = 0
  for (let i = 0; i < trace.txns.length; i++) {
    const txn = trace.txns[i]

    const parents = txn.parents.map(idx => lastLVOfTxn[idx])
    const seqStart = nextSeqForAgent[txn.agent]
    // The "length" of the transaction. Every delete and insert counts for 1.
    const len = txn.patches.reduce((prev, [_pos, delHere, insContent]) => {
      return prev + delHere + [...insContent].length
    }, 0)
    const seqEnd = seqStart + len
    nextSeqForAgent[txn.agent] = seqEnd

    causalGraph.add(cg, `${txn.agent}`.padStart(7, ' '), seqStart, seqEnd, parents)

    // Then the ops. They need to be shattered for now, since I'm not storing them RLE.
    for (let [pos, delHere, insContent] of txn.patches) {
      // console.log(pos, delHere, insContent)
      if ((delHere > 0) === (insContent !== '')) throw Error('Patches must always be an insert or delete')

      if (delHere > 0) {
        for (let i = 0; i < delHere; i++) {
          // The deletes all happen at the same position.
          ops.push({type: ListOpType.Del, pos})
        }
      } else {
        for (const c of insContent) {
          ops.push({type: ListOpType.Ins, pos, content: c})
          pos++
        }
      }
    }

    nextLV += len
    lastLVOfTxn[i] = nextLV - 1
  }

  return {ops, cg}
}

function check1(oplog: ListOpLog, expectedResult: string, verbose: boolean) {
  if (verbose) console.log('processing', oplog.ops.length, 'ops...')
  const start = Date.now()
  let result = ''
  for (let i = 0; i < 1; i++) result = mergeString(oplog)
  const end = Date.now()
  // fs.writeFileSync('out.txt', result)
  // console.log('Wrote output to out.txt. Took', end - start, 'ms')
  if (verbose) console.log('Generated output in', end - start, 'ms')

  try {
    assert.equal(result, expectedResult)
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

  const data: DTExport = JSON.parse(fs.readFileSync('testdata/git-makefile-raw.json', 'utf-8'))
  // const data: DTExport = JSON.parse(fs.readFileSync('testdata/ff-raw.json', 'utf-8'))
  const oplog = importOpLogOld(data)

  check1(oplog, data.endContent, true)

  // console.log(oplog.cg)

  // for (const w of walkCG(cg)) {
  //   console.log('w', w)
  // }

  // console.log('processing', oplog.ops.length, 'ops...')
  // const start = Date.now()
  // let result = ''
  // for (let i = 0; i < 1; i++) result = mergeString(oplog)
  // const end = Date.now()
  // fs.writeFileSync('out.txt', result)

  // console.log('Wrote output to out.txt. Took', end - start, 'ms')
  // assert.equal(result, data.endContent)
  // console.log('result', result)
}

// debugCheck()

function conformance() {
  const runs: DTExport[] = JSON.parse(fs.readFileSync('conformance.json', 'utf-8'))
  console.log(`Running ${runs.length} conformance tests...`)

  for (const data of runs) {
    const oplog = importOpLogOld(data)
    check1(oplog, data.endContent, false)
    // console.log(data.endContent)
  }

  console.log('All tests pass!')
}

// conformance()

// ;(() => {
//   const data: DTOpLogItem[] = JSON.parse(fs.readFileSync('git-makefile.json', 'utf-8'))
//   const oplog1 = importOpLogOld(data)

//   const data2: ConcurrentTrace = JSON.parse(fs.readFileSync('testdata/git-makefile.json', 'utf-8'))
//   const oplog2 = importFromConcurrentTrace(data2)

//   const ab: Record<string, string> = {}
//   const ba: Record<string, string> = {}
//   for (let i = 0; i < oplog1.cg.entries.length; i++) {
//     const e1 = oplog1.cg.entries[i], e2 = oplog2.cg.entries[i]

//     const a1 = e1.agent, a2 = e2.agent
//     if (ab[a1] == null) { ab[a1] = a2 } else {
//       assert.equal(ab[a1], a2)
//     }
//     if (ba[a2] == null) { ba[a2] = a1 } else {
//       assert.equal(ba[a2], a1)
//     }

//     // console.log(e1, e2)
//     assert.deepEqual({...e1, agent:''}, {...e2, agent:''})
//   }

//   assertSorted(Object.keys(ab).sort().map(a => ab[a]).map(x => Number(x)))

//   assert.deepEqual(oplog1.ops, oplog2.ops)
//   assert.deepEqual(oplog1.cg.entries.length, oplog2.cg.entries.length)
//   // assert.deepEqual(oplog1.cg., oplog2.ops)

// })()

const trimCG = (cg: causalGraph.CausalGraph, n: number) => {
  const result = causalGraph.createCG()
  for (let entry of cg.entries) {
    let len = entry.vEnd - entry.version

    if (n < len) {
      // Trim the entry.
      entry = {
        ...entry,
        vEnd: entry.version + n
      }
      len = n
    }

    causalGraph.add(result, entry.agent, entry.seq, entry.seq + len, entry.parents)
    n -= len
    if (n <= 0) break
  }

  return result
}

function xxx() {
  const data: DTExport = JSON.parse(fs.readFileSync('testdata/git-makefile-raw.json', 'utf-8'))
  const oplog = importOpLogOld(data)
  // const data: ConcurrentTrace = JSON.parse(fs.readFileSync('testdata/git-makefile.json', 'utf-8'))
  // const oplog = importFromConcurrentTrace(data)


  interface SplatData {
    numOps: number,
    f: number[],
    r: RawVersion[],
    result: string,
  }
  const splat: SplatData[] = JSON.parse(fs.readFileSync('splat.json', 'utf-8'))

  for (const s of splat) {
    console.log('s', s.numOps)
    // Make an abridged oplog.
    const smallOplog: ListOpLog = {
      cg: trimCG(oplog.cg, s.numOps),
      ops: oplog.ops.slice(0, s.numOps),
    }

    const actual = mergeString(smallOplog)
    if (actual !== s.result) {
      console.log(s.f, smallOplog.cg.heads)
      console.log(s.r)
      console.log(causalGraph.lvToRawList(smallOplog.cg))

      fs.writeFileSync('a.txt', actual)
      fs.writeFileSync('b.txt', s.result)

      // console.log('expected', s.result)
      // console.log('actual', actual)
      throw Error('results dont match. written actual and expected to a.txt, b.txt')
    }
  }
  // console.log(splat[1])
}

// xxx()
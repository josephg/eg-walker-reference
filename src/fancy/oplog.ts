// An oplog is a collection of operations made by one or multiple users.
//
// This is intentionally not super optimized.

import * as causalGraph from "../causal-graph.js";

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

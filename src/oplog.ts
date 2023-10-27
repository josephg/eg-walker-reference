// An oplog is a collection of operations made by one or multiple users.
//
// This is intentionally not super optimized.

import * as fs from 'node:fs'
import * as causalGraph from "./causal-graph.js";
import { LV, LVRange } from "./types.js";
import { assert } from './utils.js';
import { mergeString } from './merge.js';

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



// This code is for testing the algorithm.

// This is the data format output from the `dt export` command.
interface DTOpLogItem {
  agent: string,
  // LV.
  span: LVRange,
  parents: LV[],

  ops: [pos: number, del: number, insContent: string][],
}

function importOpLog(items: DTOpLogItem[]): ListOpLog {
  const ops: ListOp[] = []
  const cg = causalGraph.createCG()

  // I'm going to reuse the LVs from diamond types directly.
  // But we need to shatter them.
  const nextSeqForAgent: Record<string, number> = {}

  for (const item of items) {
    const seqStart = nextSeqForAgent[item.agent] ?? 0
    const len = item.span[1] - item.span[0]
    const seqEnd = seqStart + len
    causalGraph.add(cg, item.agent, seqStart, seqEnd, item.parents)

    nextSeqForAgent[item.agent] = seqEnd

    // Then the ops. They need to be shattered for now, since I'm not storing them RLE.
    for (let [pos, delHere, insContent] of item.ops) {
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


;(() => {
  // const data = JSON.parse(fs.readFileSync('am.json', 'utf-8'))
  // const data = JSON.parse(fs.readFileSync('testdata/node_nodecc.json', 'utf-8'))
  const data = JSON.parse(fs.readFileSync('testdata/ff.json', 'utf-8'))
  const oplog = importOpLog(data)
  // console.log(oplog.cg)

  // for (const w of walkCG(cg)) {
  //   console.log('w', w)
  // }

  const start = Date.now()
  const result = mergeString(oplog)
  const end = Date.now()
  fs.writeFileSync('out.txt', result)
  console.log('Wrote output to out.txt. Took', end - start, 'ms')
  // console.log('result', result)
})()


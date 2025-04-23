import * as causalGraph from "../src/causal-graph.js"
import type { LV, LVRange } from "../src/causal-graph.js"

import { ListOp, ListOpLog, checkoutSimpleString, checkoutSimulated, createOpLog, opLen, pushRemoteOp } from '../src/index.js';
import { assertEq } from "../src/utils.js";
import * as testData from '../testdata/C1.json'

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
        ? {type: 'del', pos, len: delHere}
        : {type: 'ins', pos, content: [...insContent]}

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

const oplog = importDTOpLog(testData.default as DTExport)
console.log('ok')
console.time('checkout')
console.profile('checkout')
// checkoutSimpleString(oplog)
for (let i = 0; i < 10; i++) checkoutSimulated(oplog)
console.profileEnd('checkout')
console.timeEnd('checkout')
console.log('done!')

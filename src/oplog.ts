// An oplog is a collection of operations made by one or multiple users.
//
// This is intentionally not super optimized.

import * as fs from 'node:fs'
import * as causalGraph from "./causal-graph.js";
import { LV, LVRange } from "./types.js";
import { assert } from './utils.js';

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


const enum ItemState {
  NotYetInserted = -1,
  Inserted = 0,
  Deleted = 1, // Or some +ive number of times the item has been deleted.
}

interface CRDTListItem {
  lv: LV,
  /** The item's state at this point in the merge */
  curState: ItemState,
  /** The item's state when *EVERYTHING* has been merged */
  endState: ItemState,

  // -1 means start / end of document.
  originLeft: LV | -1,
  originRight: LV | -1,
}

interface EditContext<T = any> {
  items: CRDTListItem[],

  // delTarget[del_lv] = target_lv.
  delTargets: LV[],

  // dest: string | any[],

  // Note even for strings, I'm using an array of (unicode) characters here to make
  // dealing with unicode easier.
  dest: T[],
}



interface WalkItem {
  retreat: LVRange[],
  advance: LVRange[],
  consume: LVRange,
}

/**
 * This essentially does a depth-first traversal of the causal graph to generate
 * our plan for how to apply all the operations.
 */
function *walkCG(cg: causalGraph.CausalGraph): Generator<WalkItem> {
  // Our current location.
  // let curFrontier: LV[] = []
  let curLV = -1

  // The cg entries are already topologically sorted. Small improvements
  // in this traversal plan result in much faster merging, but even so
  // for simplicity I'm just going to keep the order in cg.entries and walk in order.
  for (const s of cg.entries) {
    if ((s.parents.length === 0 && curLV === -1)
        || (s.parents.length === 1 && curLV === s.parents[0])) {
      // We can just directly process this item in sequence.
      yield {
        retreat: [], advance: [], consume: [s.version, s.vEnd]
      }
      // curFrontier.length = 1
      // curFrontier[0] = s.vEnd - 1
    } else {
      const {aOnly, bOnly} = causalGraph.diff(cg, [curLV], s.parents)
      yield {
        retreat: aOnly,
        advance: bOnly,
        consume: [s.version, s.vEnd],
      }
    }
    curLV = s.vEnd - 1
  }
}



interface DocCursor {
  idx: number,
  curPos: number,
  endPos: number,
}

function advance1(ctx: EditContext, oplog: ListOpLog<string>, lv: LV) {
  const op = oplog.ops[lv]
  // const cursor = findByLV(ctx, lv)
  // const item = ctx.items[cursor.idx]

  // For inserts, the item being reactivated is just the op itself. For deletes,
  // we need to look up the item in delTargets.
  const targetLV = op.type === ListOpType.Del ? ctx.delTargets[lv] : lv
  const item = ctx.items.find(i => i.lv === targetLV)!

  if (op.type === ListOpType.Del) {
    assert(item.curState === ItemState.Inserted, 'Invalid state - adv Del but item is not Ins')
    assert(item.endState >= ItemState.Deleted, 'Advance delete with item not deleted in endState')
    item.curState = ItemState.Deleted
  } else {
    // Mark the item as inserted.
    assert(item.curState === ItemState.NotYetInserted, 'Advance insert for already inserted item')
    item.curState = ItemState.Inserted
    // item.curState = ItemState.Inserted // Also equivalent to item.mergeState++.
  }
}

function retreat1(ctx: EditContext, oplog: ListOpLog<string>, lv: LV) {
  const op = oplog.ops[lv]
  // const cursor = findByLV(ctx, lv)
  // const item = ctx.items[cursor.idx]
  const targetLV = op.type === ListOpType.Del ? ctx.delTargets[lv] : lv
  const item = ctx.items.find(i => i.lv === targetLV)!

  if (op.type === ListOpType.Del) {
    // Undelete the item.
    assert(item.curState >= ItemState.Deleted, 'Retreat delete but item not currently deleted')
    assert(item.endState >= ItemState.Deleted, 'Retreat delete but item not deleted')
  } else {
    // Un-insert this item.
    assert(item.curState === ItemState.Inserted, 'Retreat insert for item not in inserted state')
  }

  item.curState--
}


const itemWidth = (state: ItemState): number => state === ItemState.Inserted ? 1 : 0

function findByCurPos(ctx: EditContext, targetPos: number): DocCursor {
  // for (let i = 0; i < ctx.items.length; i++) {
  //   if (ctx.items.
  // }

  let curPos = 0
  let endPos = 0
  let i = 0

  while (curPos < targetPos) {
    if (i >= ctx.items.length) throw Error('Document is not long enough to find targetPos')

    const item = ctx.items[i]
    curPos += itemWidth(item.curState)
    endPos += itemWidth(item.endState)

    i++
  }

  return { idx: i, curPos, endPos }
}

const findItemIdx = (ctx: EditContext, needle: LV) => {
  const idx = ctx.items.findIndex(i => i.lv === needle)
  if (idx === -1) throw Error('Could not find needle in items')
  return idx
}

/**
 * YjsMod, stolen and adapted from reference-crdts. Returns the inserted endPos.
 */
const integrateYjsMod = <T>(ctx: EditContext<T>, cg: causalGraph.CausalGraph, newItem: CRDTListItem, cursor: DocCursor, left: number, right: number): number => {
  // let left = findItem(doc, newItem.originLeft, idx_hint - 1)
  // let destIdx = left + 1

  // let right = newItem.originRight == null ? doc.content.length : findItem(doc, newItem.originRight, idx_hint)
  let scanning = false

  let destIdx = 0
  let destEndPos = 0

  let scanIdx = cursor.idx
  let scanEndPos = cursor.endPos

  for (; ; scanIdx++) {
    if (!scanning) {
      destIdx = scanIdx
      destEndPos = scanEndPos
    }

    if (scanIdx === ctx.items.length) break // We've reached the end of the document. Insert.

    // let other = doc.content[i]
    let other = ctx.items[scanIdx]
    if (other.lv === newItem.originRight) break // End of the concurrent range. Insert.

    let oleft = other.originLeft === -1 ? -1 : findItemIdx(ctx, other.originLeft)
    let oright = other.originRight === -1 ? ctx.items.length : findItemIdx(ctx, other.originRight)

    // The logic below summarizes to:
    // if (oleft < left || (oleft === left && oright === right && newItem.id[0] < o.id[0])) break
    // if (oleft === left) scanning = oright < right

    // Ok now we implement the punnet square of behaviour
    if (oleft < left) {
      // Top row. Insert, insert, arbitrary (insert)
      break
    } else if (oleft === left) {
      // Middle row.
      if (oright < right) {
        // This is tricky. We're looking at an item we *might* insert after - but we can't tell yet!
        scanning = true
        continue
      } else if (oright === right) {
        // Raw conflict. Order based on user agents.
        if (causalGraph.lvCmp(cg, newItem.lv, other.lv) < 0) {
          break
        } else {
          scanning = false
          continue
        }
      } else { // oright > right
        scanning = false
        continue
      }
    } else { // oleft > left
      // Bottom row. Arbitrary (skip), skip, skip
      continue
    }
  }

  // We've found the position. Insert here.
  ctx.items.splice(destIdx, 0, newItem)
  return destEndPos
}

function apply1(ctx: EditContext, oplog: ListOpLog<string>, lv: LV) {
  // This integrates the op into the document. This code is copied from reference-crdts.
  const op = oplog.ops[lv]

  if (op.type === ListOpType.Del) {
    // This is simple. We just need to mark the item as deleted and delete it from the output.
    const cursor = findByCurPos(ctx, op.pos)
    // Find the next item which we can actually delete.
    // This will crash if we fall off the end of the items list. Thats ok - that means
    // the data is invalid or we've messed something up somewhere.
    while (ctx.items[cursor.idx].curState !== ItemState.Inserted) {
      const item = ctx.items[cursor.idx]
      cursor.curPos += itemWidth(item.curState)
      cursor.endPos += itemWidth(item.endState)
      cursor.idx++
    }

    const item = ctx.items[cursor.idx]
    // Note this item may have already been deleted by a concurrent edit. In that case, endState
    // will already be Deleted.
    assert(item.curState === ItemState.Inserted, 'Trying to delete an item which is not currently Inserted')

    // Delete it in the output.
    if (item.endState === ItemState.Inserted) {
      ctx.dest.splice(cursor.endPos, 1)
    }

    // And mark the item as deleted.
    item.curState++ // Inserted -> Deleted.
    item.endState++ // This will set it to be Deleted or double deleted. We don't actually need to count how many times.

    // And mark that this delete corresponds to *that* item.
    ctx.delTargets[lv] = item.lv
  } else {
    // Insert! This is much more complicated as we need to do the Yjs integration.
    const cursor = findByCurPos(ctx, op.pos)
    // The cursor position is at the first valid insert location.
    if (cursor.idx > 0) {
      // Its valid because the previous item must be inserted in the current state.
      const prevItem = ctx.items[cursor.idx - 1]
      assert(prevItem.curState === ItemState.Inserted)
    }

    // Anyway, originLeft is just the LV of the item to our left.
    const originLeft = cursor.idx === 0 ? -1 : ctx.items[cursor.idx - 1].lv

    // originRight is the ID of the next item which isn't in the NYI curState.
    let originRight = -1
    let rightIdx = cursor.idx
    for (; rightIdx < ctx.items.length; rightIdx++) {
      const nextItem = ctx.items[rightIdx]
      if (nextItem.curState !== ItemState.NotYetInserted) {
        originRight = nextItem.lv
        break
      }
    } // If we run out of items, originRight is just -1 (as above) and rightIdx is ctx.items.length.

    const newItem: CRDTListItem = {
      curState: ItemState.Inserted,
      endState: ItemState.Inserted,
      lv,
      originLeft,
      originRight
    }

    // This will insert the new item into ctx.items and return the endPos.
    const endPos = integrateYjsMod(ctx, oplog.cg, newItem, cursor, cursor.idx - 1, rightIdx)

    // And finally, actually insert it in the resulting document.
    ctx.dest.splice(endPos, 0, op.content!)
  }
}

function mergeString(oplog: ListOpLog<string>): string {
  const ctx: EditContext = {
    items: [],
    delTargets: new Array(oplog.ops.length).fill(-1),
    dest: [],
  }

  for (const {retreat, advance, consume} of walkCG(oplog.cg)) {
    // Retreat.
    // Note we're processing these in reverse order to make sure items
    // are undeleted before being un-inserted.
    for (let i = retreat.length - 1; i >= 0; i--) {
      const [start, end] = retreat[i]
      for (let lv = end - 1; lv >= start; lv--) {
        // console.log('retreat1', lv, oplog.ops[lv].type)
        retreat1(ctx, oplog, lv)
      }
    }

    // Advance.
    for (const [start, end] of advance) {
      for (let lv = start; lv < end; lv++) {
        advance1(ctx, oplog, lv)
      }
    }

    // Then apply the operation.
    const [start, end] = consume
    for (let lv = start; lv < end; lv++) {
      // console.log('apply1', lv, oplog.ops[lv].type)
      apply1(ctx, oplog, lv)
    }
  }

  return ctx.dest.join('')
}



// This comes from the `dt export` command.
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

  // console.log('cg', cg)
  // console.log('ops', ops)

  return {ops, cg}
}


;(() => {
  // const data = JSON.parse(fs.readFileSync('am.json', 'utf-8'))
  const data = JSON.parse(fs.readFileSync('testdata/ff.json', 'utf-8'))
  const oplog = importOpLog(data)
  // console.log(oplog.cg)

  // for (const w of walkCG(cg)) {
  //   console.log('w', w)
  // }

  const result = mergeString(oplog)
  fs.writeFileSync('out.txt', result)
  console.log('Wrote output to out.txt')
  // console.log('result', result)
})()
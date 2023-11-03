// This implements the core algorithm in as readable a form as I can manage.
//
// This code has some simplifications from the more full implementation:
//
// - There's no support here for merging new changes into a document snapshot
//
// And its missing "standard" performance optimizations I'd want from a real
// library:
//
// - There's no run-length encoding (well, outside the causal-graph library).
//   All operations are split into individual insert / deletes.
// - When we replay operations, *all* operations are processed in full. A
//   better library will skip updating the CRDT when operations are fully ordered.
// - The causal graph is traversed naively

// But because I'm a bit lazy, I'm reusing the fancier causal graph implementation.
// This implementation internally run-length encodes the causal graph in memory.
//
// This library is used for its graph manipulation helper functions - like diff
// and iterVersionsBetween.
import * as causalGraph from "../causal-graph.js"
import { assert, assertEq } from '../utils.js'

/**
 * Operations either insert new content at some position (index), or delete the item
 * at some position.
 *
 * Note the positions are normal array / string indexes, indexing into what the
 * document looked like when the operation was created (at its parent version).
 *
 * Operations also have an ID (agent,seq pair) and a list of parent versions. In this
 * implementation, the ID and parents are stored separately - in the causal graph.
*/
export type SimpleListOp<T = any> = {
  type: 'ins',
  pos: number
  content: T
} | {
  type: 'del',
  pos: number,
}

export interface SimpleListOpLog<T = any> {
  // The LV for each op is its index in this list.
  ops: SimpleListOp<T>[],
  cg: causalGraph.CausalGraph,
}

export function createSimpleOpLog<T = any>(): SimpleListOpLog<T> {
  return {
    ops: [],

    // The causal graph stores the IDs (agent,seq) and parents for each
    // of the operations.
    cg: causalGraph.createCG()
  }
}

export function localInsert<T>(oplog: SimpleListOpLog<T>, agent: string, pos: number, content: T) {
  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+1, oplog.cg.heads)
  oplog.ops.push({ type: 'ins', pos, content })
}

export function localDelete<T>(oplog: SimpleListOpLog<T>, agent: string, pos: number, len: number = 1) {
  if (len === 0) throw Error('Invalid delete length')

  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+len, oplog.cg.heads)
  for (let i = 0; i < len; i++) {
    oplog.ops.push({ type: 'del', pos })
  }
}

/**
 * This function adds everything in the src oplog to dest.
 */
export function mergeOplogInto<T>(dest: SimpleListOpLog<T>, src: SimpleListOpLog<T>) {
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

enum ItemState {
  NotYetInserted = -1,
  Inserted = 0,
  Deleted = 1, // Or some +ive number of times the item has been deleted.
}

// This is internal only, and used while reconstructing the changes.
interface Item {
  opId: number,

  /**
   * The item's state at this point in the merge. This is initially set to Inserted,
   * but if we reverse the operation out we'll end up in NotYetInserted. And if the item
   * is deleted multiple times (by multiple concurrent users), we'll end up storing the
   * number of times the item was deleted here.
   */
  curState: ItemState,

  /**
   * The item's state when *EVERYTHING* has been merged. This is always either Inserted or Deleted.
   */
  endState: ItemState,

  // -1 means start / end of document. This is the core list CRDT (sync9/fugue).
  originLeft: number | -1,

  // -1 means the end of the document. This uses fugue's semantics.
  // All right children have a rightParent of -1.
  rightParent: number | -1,
}

interface EditContext {
  // All the items in document order. This list is grow-only, and will be spliced()
  // in as needed.
  items: Item[],

  // When we delete something, we store the LV of the item that was deleted. This is
  // used when items are un-deleted (and re-deleted).
  // delTarget[del_lv] = target_lv.
  delTargets: number[],

  // This is the same set of items as above, but this time indexed by LV. This is
  // used to make it fast & easy to activate and deactivate items.
  itemsByLV: Item[],
}

function advance1<T>(ctx: EditContext, oplog: SimpleListOpLog<T>, opId: number) {
  const op = oplog.ops[opId]

  // For inserts, the item being reactivated is just the op itself. For deletes,
  // we need to look up the item in delTargets.
  const targetLV = op.type === 'del' ? ctx.delTargets[opId] : opId
  const item = ctx.itemsByLV[targetLV]

  if (op.type === 'del') {
    assert(item.curState >= ItemState.Inserted, 'Invalid state - adv Del but item is ' + item.curState)
    assert(item.endState >= ItemState.Deleted, 'Advance delete with item not deleted in endState')
    item.curState++
  } else {
    // Mark the item as inserted.
    assertEq(item.curState, ItemState.NotYetInserted, 'Advance insert for already inserted item ' + opId)
    item.curState = ItemState.Inserted
  }
}

function retreat1<T>(ctx: EditContext, oplog: SimpleListOpLog<T>, opId: number) {
  const op = oplog.ops[opId]
  const targetLV = op.type === 'del' ? ctx.delTargets[opId] : opId
  const item = ctx.itemsByLV[targetLV]

  if (op.type === 'del') {
    // Undelete the item.
    assert(item.curState >= ItemState.Deleted, 'Retreat delete but item not currently deleted')
    assert(item.endState >= ItemState.Deleted, 'Retreat delete but item not deleted')
  } else {
    // Un-insert this item.
    assertEq(item.curState, ItemState.Inserted, 'Retreat insert for item not in inserted state')
  }

  item.curState--
}


const itemWidth = (state: ItemState): number => state === ItemState.Inserted ? 1 : 0

interface DocCursor {
  idx: number,
  endPos: number,
}

function findByCurPos(ctx: EditContext, targetPos: number): DocCursor {
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

  return { idx: i, endPos }
}

const findItemIdx = (ctx: EditContext, needle: number): number => {
  const idx = ctx.items.findIndex(i => i.opId === needle)
  if (idx === -1) throw Error('Could not find needle in items')
  return idx
}

/**
 * This function is called when we process insert operations to find (via a scan) the
 * correct location in the item list to insert the new item. The location is passed
 * back to the caller via the (modified) cursor.
 *
 * Some history:
 *
 * This algorithm started its life in Yjs, written by Kevin Jahns. I adapted that approach
 * for reference-crdts. Then modified & improved it in place to make YjsMod. Then the
 * algorithm was improved further by Matthew Weidner to make Fugue. The fugue paper
 * proves many nice properties about this algorithm and its interleaving behaviour:
 *
 * https://arxiv.org/abs/2305.00583
 *
 * Finally, I've replaced rightOrigin in fugue-list's items implementation with rightParent.
 * (Calculated before integrate is called). Ordinarily this would result in correct behaviour
 * but pathological performance in the integrate method because rightOrigin is also used in
 * fugue-list to bound the search for insert location. But, here its easy to simply calculate
 * rightBound as items are inserted.
 *
 * Meanwhile, Greg Little and Michael Toomim wrote a separate sequence CRDT algorithm called
 * Sync9. Sync9 predated fugue by a couple of years. It was formulated a different way (using
 * trees), but it turns out the ordering behaviour between sync9 and fugue is identical.
 *
 * Anyway, the long and short of it is: This function implements the Sync9 / Fugue CRDT.
 */
function integrate(ctx: EditContext, cg: causalGraph.CausalGraph, newItem: Item, cursor: DocCursor) {
  // If there's no concurrency, we don't need to scan.
  if (cursor.idx >= ctx.items.length || ctx.items[cursor.idx].curState !== ItemState.NotYetInserted) return

  // Sometimes we need to scan ahead and maybe insert there, or maybe insert here.
  let scanning = false
  let scanIdx = cursor.idx
  let scanEndPos = cursor.endPos

  const leftIdx = cursor.idx - 1
  const rightIdx = newItem.rightParent === -1 ? ctx.items.length : findItemIdx(ctx, newItem.rightParent)

  while (scanIdx < ctx.items.length) {
    let other = ctx.items[scanIdx]

    // When concurrent inserts happen, the newly inserted item goes somewhere between the
    // insert position itself (passed in through cursor) to the next item that existed
    // when which the insert occurred. We can use the item's state to bound the search.
    if (other.curState !== ItemState.NotYetInserted) break

    if (other.opId === newItem.rightParent) throw Error('invalid state')

    // The index of the origin left / right for the other item.
    let oleftIdx = other.originLeft === -1 ? -1 : findItemIdx(ctx, other.originLeft)
    if (oleftIdx < leftIdx) break
    else if (oleftIdx === leftIdx) {
      let orightIdx = other.rightParent === -1 ? ctx.items.length : findItemIdx(ctx, other.rightParent)

      if (orightIdx === rightIdx && causalGraph.lvCmp(cg, newItem.opId, other.opId) < 0) break
      else scanning = orightIdx < rightIdx
    }

    scanEndPos += itemWidth(other.endState)
    scanIdx++

    if (!scanning) {
      cursor.idx = scanIdx
      cursor.endPos = scanEndPos
    }
  }

  // We've found the position. Insert where the cursor points.
}

function apply1<T>(ctx: EditContext, dest: T[], oplog: SimpleListOpLog<T>, opId: number) {
  // This integrates the op into the document. This code is copied from reference-crdts.
  const op = oplog.ops[opId]

  if (op.type === 'del') {
    // This is simple. We just need to mark the item as deleted and delete it from the output.
    const cursor = findByCurPos(ctx, op.pos)
    // Find the next item which we can actually delete.
    // This will crash if we fall off the end of the items list. Thats ok - that means
    // the data is invalid or we've messed something up somewhere.
    while (ctx.items[cursor.idx].curState !== ItemState.Inserted) {
      const item = ctx.items[cursor.idx]
      cursor.endPos += itemWidth(item.endState)
      cursor.idx++
    }

    const item = ctx.items[cursor.idx]
    // Note this item may have already been deleted by a concurrent edit. In that case, endState
    // will already be Deleted.
    assert(item.curState === ItemState.Inserted, 'Trying to delete an item which is not currently Inserted')

    // Delete it in the output.
    if (item.endState === ItemState.Inserted) {
      dest.splice(cursor.endPos, 1)
    }

    // And mark the item as deleted. For the curState, we can't get into a "double deletes"
    // state here, because item.curState must be Inserted right now. For endState, we
    // don't care about double deletes at all.
    item.curState = item.endState = ItemState.Deleted

    // And mark that this delete corresponds to *that* item.
    ctx.delTargets[opId] = item.opId
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
    const originLeft = cursor.idx === 0 ? -1 : ctx.items[cursor.idx - 1].opId

    // originRight is the ID of the next item which isn't in the NYI curState.
    let rightParent = -1

    // Scan to find the newly inserted item's right parent.
    for (let i = cursor.idx; i < ctx.items.length; i++) {
      const nextItem = ctx.items[i]
      if (nextItem.curState !== ItemState.NotYetInserted) {
        // We'll take this item for the "right origin" and right bound (highest index) that we can insert at.
        rightParent = (nextItem.originLeft === originLeft) ? nextItem.opId : -1
        break
      }
    } // If we run out of items, originRight is just -1 (as above) and rightIdx is ctx.items.length.

    const newItem: Item = {
      curState: ItemState.Inserted,
      endState: ItemState.Inserted,
      opId: opId,
      originLeft,
      rightParent,
    }
    ctx.itemsByLV[opId] = newItem

    // This will update the cursor to find the location we want to insert the item.
    integrate(ctx, oplog.cg, newItem, cursor)

    ctx.items.splice(cursor.idx, 0, newItem)

    // And finally, actually insert it in the resulting document.
    dest.splice(cursor.endPos, 0, op.content!)
  }
}

// This is a helper debugging function, for printing out the internal state of the
// editing context.
function debugPrintCtx<T>(ctx: EditContext, oplog: SimpleListOpLog<T>) {
  console.log('---- DT STATE ----')

  const depth: Record<number, number> = {}
  depth[-1] = 0

  for (const item of ctx.items) {
    const isLeftChild = true
    const parent = isLeftChild ? item.originLeft : item.rightParent
    const d = parent === -1 ? 0 : depth[parent] + 1

    depth[item.opId] = d
    const lvToStr = (lv: number) => {
      if (lv === -1) return 'ROOT'
      const rv = causalGraph.lvToRaw(oplog.cg, lv)
      return `[${rv[0]},${rv[1]}]`
    }

    const op = oplog.ops[item.opId]
    if (op.type !== 'ins') throw Error('Invalid state') // This avoids a typescript type error.
    const value = item.endState === ItemState.Deleted ? null : op.content

    let content = `${value == null ? '.' : value} at ${lvToStr(item.opId)} (left ${lvToStr(item.originLeft)})`
    content += ` right ${lvToStr(item.rightParent)}`
    console.log(`${'| '.repeat(d)}${content}`)
  }
}

export function checkoutSimple<T>(oplog: SimpleListOpLog<T>): T[] {
  const ctx: EditContext = {
    items: [],
    delTargets: new Array(oplog.ops.length).fill(-1),
    itemsByLV: new Array(oplog.ops.length).fill(null),
  }

  // The version we're currently at while processing
  let curVersion: number[] = []

  // The resulting document snapshot
  const data: T[] = []

  // What we need to do here is walk through all the operations
  // one by one. When we get to each operation, if the current version
  // is different from the operation's parents, we'll "move" to that
  // version by disabling and re-enabling some of the operations we've
  // already processed.

  // Any topological-ordered traversal of the causal graph will work,
  // and generate the right output. A better implementation can optimize
  // the traversal order in order to call retreat/advance fewer times.
  // But here I'll just process all the operations in the order we're
  // storing them in, since thats easier. And they're already stored in a
  // topologically sorted order.
  for (const entry of causalGraph.iterVersionsBetween(oplog.cg, 0, causalGraph.nextLV(oplog.cg))) {
    const {aOnly, bOnly} = causalGraph.diff(oplog.cg, curVersion, entry.parents)

    // The causal graph library run-length encodes everything.
    // These are all ranges of operations.
    const retreat = aOnly
    const advance = bOnly
    const consume = [entry.version, entry.vEnd] // Operations to apply.

    // After processing these operations, we're at the last version in the range.
    curVersion = [entry.vEnd - 1]

    // Retreat.
    // Note we're processing these in reverse order to make sure items
    // are undeleted before being un-inserted.
    for (let i = retreat.length - 1; i >= 0; i--) {
      const [start, end] = retreat[i]
      for (let lv = end - 1; lv >= start; lv--) {
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
      apply1(ctx, data, oplog, lv)
    }
  }

  return data
}

export function mergeString(oplog: SimpleListOpLog<string>): string {
  return checkoutSimple(oplog).join('')
}

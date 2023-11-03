// This file contains the code to find the final document state based on an oplog.
import * as causalGraph from "../causal-graph.js"
import { ID, ListFugueSimple } from "../list-fugue-simple.js"
import { ListOp, ListOpLog, ListOpType } from "./oplog.js"
import { Branch, LV, LVRange } from "../types.js"
import { assert, assertEq } from '../utils.js'
import {deepEqual} from 'node:assert/strict'

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

interface WalkItem {
  // These ranges are ranges of opIds. They're half-open - so they include the start,
  // but not the end.
  retreat: [number, number][],
  advance: [number, number][],
  consume: [number, number],
}

/**
 * This essentially does a depth-first traversal of the causal graph to generate
 * our plan for how to apply all the operations.
 *
 * This is a naive traversal. Pathological cases may significantly slow down processing.
 * But its simple and correct.
 */
function *walkCG(cg: causalGraph.CausalGraph, curLV: number[] = [], vStart: number = 0, vEnd: number = causalGraph.nextLV(cg)): Generator<WalkItem, number[]> {
  // Our current location.
  // let curFrontier: LV[] = []

  // The cg entries are already topologically sorted. Small improvements
  // in this traversal plan result in much faster merging, but even so
  // for simplicity I'm just going to keep the order in cg.entries and walk in order.
  for (const entry of causalGraph.iterVersionsBetween(cg, vStart, vEnd)) {
    if (causalGraph.lvEq(curLV, entry.parents)) {
      // We can just directly process this item in sequence.
      yield {
        retreat: [], advance: [], consume: [entry.version, entry.vEnd]
      }
      // curFrontier.length = 1
      // curFrontier[0] = s.vEnd - 1
    } else {
      const {aOnly, bOnly} = causalGraph.diff(cg, curLV, entry.parents)
      yield {
        retreat: aOnly,
        advance: bOnly,
        consume: [entry.version, entry.vEnd],
      }
    }
    curLV = [entry.vEnd - 1]
  }

  return curLV
}



interface DocCursor {
  idx: number,
  endPos: number,
}

function advance1<T>(ctx: EditContext, oplog: ListOpLog<T>, opId: number) {
  const op = oplog.ops[opId]
  // const cursor = findByLV(ctx, lv)
  // const item = ctx.items[cursor.idx]

  // For inserts, the item being reactivated is just the op itself. For deletes,
  // we need to look up the item in delTargets.
  const targetLV = op.type === ListOpType.Del ? ctx.delTargets[opId] : opId
  // const item = ctx.items.find(i => i.lv === targetLV)!
  const item = ctx.itemsByLV[targetLV]

  if (op.type === ListOpType.Del) {
    assert(item.curState >= ItemState.Inserted, 'Invalid state - adv Del but item is ' + item.curState)
    assert(item.endState >= ItemState.Deleted, 'Advance delete with item not deleted in endState')
    // item.curState = ItemState.Deleted
    item.curState++
  } else {
    // Mark the item as inserted.
    assertEq(item.curState, ItemState.NotYetInserted, 'Advance insert for already inserted item ' + opId)
    item.curState = ItemState.Inserted
    // item.curState = ItemState.Inserted // Also equivalent to item.mergeState++.
  }
}

function retreat1<T>(ctx: EditContext, oplog: ListOpLog<T>, opId: number) {
  const op = oplog.ops[opId]
  // const cursor = findByLV(ctx, lv)
  // const item = ctx.items[cursor.idx]
  const targetLV = op.type === ListOpType.Del ? ctx.delTargets[opId] : opId
  // const item = ctx.items.find(i => i.lv === targetLV)!
  const item = ctx.itemsByLV[targetLV]

  if (op.type === ListOpType.Del) {
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

const findItemIdx = (ctx: EditContext, needle: number) => {
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

function apply1<T>(ctx: EditContext, dest: T[] | null, oplog: ListOpLog<T>, opId: number, fugue?: ListFugueSimple<T>) {
  const opIdToFugueId = (id: number): ID | null => {
    if (id === -1) return null
    const rawId = causalGraph.lvToRaw(oplog.cg, id)
    return { sender: rawId[0], counter: rawId[1] }
  }

  // if (opId > 0 && opId % 10000 === 0) console.log(opId, '...')

  // This integrates the op into the document. This code is copied from reference-crdts.
  const op = oplog.ops[opId]

  if (op.type === ListOpType.Del) {
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
      if (dest) dest.splice(cursor.endPos, 1)
    }

    // And mark the item as deleted. For the curState, we can't get into a "double deletes"
    // state here, because item.curState must be Inserted right now. For endState, we
    // don't care about double deletes at all.
    item.curState = item.endState = ItemState.Deleted

    // And mark that this delete corresponds to *that* item.
    ctx.delTargets[opId] = item.opId

    if (fugue != null) {
      fugue.receivePrimitive({
        type: 'delete',
        id: opIdToFugueId(item.opId)!,
      })
    }
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
    let tempOriginRight = -1 // TODO: Remove me

    // Scan to find the newly inserted item's right parent.
    for (let i = cursor.idx; i < ctx.items.length; i++) {
      const nextItem = ctx.items[i]
      if (nextItem.curState !== ItemState.NotYetInserted) {
        tempOriginRight = nextItem.opId
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
    if (ctx.itemsByLV[opId] != null) debugger
    assert(ctx.itemsByLV[opId] == null, 'Inserted item already in list')
    ctx.itemsByLV[opId] = newItem

    // This will update the cursor to find the location we want to insert the item.
    integrate(ctx, oplog.cg, newItem, cursor)

    ctx.items.splice(cursor.idx, 0, newItem)

    // And finally, actually insert it in the resulting document.
    if (dest) dest.splice(cursor.endPos, 0, op.content!)

    if (fugue != null) {
      let leftId = opIdToFugueId(originLeft) ?? fugue.start.id
      let rightId = opIdToFugueId(tempOriginRight) ?? fugue.end.id
      // console.log('inserting', opIdToFugueId(opId), 'left', leftId, 'right', rightId)
      fugue.receivePrimitive({
        type: 'insert',
        id: opIdToFugueId(opId)!,
        leftOrigin: leftId,
        rightOrigin: rightId,
        value: op.content!
      })
    }
  }
}

function debugPrintCtx<T>(ctx: EditContext, oplog: ListOpLog<T>) {
  console.log('---- DT STATE ----')

  const depth: Record<number, number> = {}
  // const kForId = (id: Id, c: T | null) => `${id[0]} ${id[1]} ${id[2] ?? c != null}`
  // const eltId = (elt: Element<any>) => elt.id.sender === '' ? 'ROOT' : `${elt.id.sender},${elt.id.counter}`
  depth[-1] = 0

  for (const item of ctx.items) {
    const isLeftChild = true
    // const isLeftChild = this.rightParent(elt.originLeft, elt.rightParent) === this.end
    const parent = isLeftChild ? item.originLeft : item.rightParent
    const d = parent === -1 ? 0 : depth[parent] + 1

    depth[item.opId] = d
    const lvToStr = (lv: number) => {
      if (lv === -1) return 'ROOT'
      const rv = causalGraph.lvToRaw(oplog.cg, lv)
      return `[${rv[0]},${rv[1]}]`
    }

    const op = oplog.ops[item.opId]
    if (op.type !== ListOpType.Ins) throw Error('Invalid state') // This avoids a typescript type error.
    const value = item.endState === ItemState.Deleted ? null : op.content

    // let content = `${isLeftChild ? '/' : '\\'}${elt.value == null
    let content = `${value == null ? '.' : value} at ${lvToStr(item.opId)} (left ${lvToStr(item.originLeft)})`
    content += ` right ${lvToStr(item.rightParent)}`
    // console.log(`${'| '.repeat(d)}${elt.value == null ? chalk.strikethrough(content) : content}`)
    console.log(`${'| '.repeat(d)}${content}`)
  }
}

function walkBetween<T>(ctx: EditContext, oplog: ListOpLog<T>, data: T[] | null, fugue?: ListFugueSimple<T>) {
  return walkBetween2(ctx, oplog, [], 0, causalGraph.nextLV(oplog.cg), data, fugue)
}


function walkBetween2<T>(ctx: EditContext, oplog: ListOpLog<T>, curLV: number[], vStart: number, vEnd: number, data: T[] | null, fugue?: ListFugueSimple<T>): LV[] {
  if (data != null) assert(data.length <= ctx.items.length)

  // This function also needs to return the resulting version after doing this
  // walk. (Which will be [vEnd - 1], but still).
  const gen = walkCG(oplog.cg, curLV, vStart, vEnd)

  let entry
  while (!(entry = gen.next()).done) {
    const {retreat, advance, consume} = entry.value
    // console.log('r', retreat, 'a', advance, 'c', consume)

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
      apply1(ctx, data, oplog, lv, fugue)
    }
  }
  return entry.value
}

export function checkoutSimple<T>(oplog: ListOpLog<T>): Branch<T> {
  const fugue = new ListFugueSimple<T>('_unused_')

  const ctx: EditContext = {
    items: [],
    delTargets: new Array(oplog.ops.length).fill(-1),
    itemsByLV: new Array(oplog.ops.length).fill(null),
  }

  const data: T[] = []
  walkBetween(ctx, oplog, data, fugue)

  // Deep weird check.
  // const expectedData = ctx.items
  //   .filter(i => i.endState === ItemState.Inserted)
  //   .map(i => oplog.ops[i.opId].content)
  // assert.deepEqual(expectedData, data)

  // console.log(data, oplog.ops.map(op => op.content), ctx.items)

  const fugueState = fugue.toArray()
  try {
    deepEqual(fugueState, data)
  } catch (e) {
    fugue.debugPrint()
    console.log()
    debugPrintCtx(ctx, oplog)
    // console.log(ctx.items)
    throw e
  }

  // console.log(ctx.items)
  // fugue.debugPrint()
  // console.log()
  // debugPrintCtx(ctx, oplog)

  return { data, version: oplog.cg.heads }
}

export function mergeChangesIntoBranch<T>(branch: Branch<T>, oplog: ListOpLog<T>, mergeVersion: number[] = oplog.cg.heads) {
  // We have an existing checkout of a list document. We want to merge some new changes in the oplog into
  // our local branch.
  //
  // How do we do that?
  //
  // Obviously we could regenerate the branch from scratch - but:
  // - That would be very slow
  // - It would require reading all the old (existing) list operations - which we want to leave on disk
  // - That approach wouldn't allow us to get the difference between old and new state, which is
  //   important for updating cursor information and things like that.
  //
  // Ideally we only want to look at the new operations in the oplog and apply those. However,
  // those new operations may be concurrent with operations we've already processed. In that case,
  // we need to populate the items list with any potentially concurrent items.
  //
  // The strategy here looks like this:
  // 1. Find the most recent common ancestor of the existing branch & changes we're merging
  // 2. Re-iterate through the "conflicting set" of changes we've already merged to populate the items list
  // 3. Process the new operations as normal, starting with the crdt items list we've just generated.

  // First lets see what we've got. I'll divide the conflicting range into two groups:
  // - The conflict set. (Stuff we've already processed that we need to process again).
  // - The new operations we need to merge
  const newOps: LVRange[] = []
  const conflictOps: LVRange[] = []

  let commonAncestor = causalGraph.findConflicting(oplog.cg, branch.version, mergeVersion, (span, flag) => {
      // Note this visitor function visits these operations in reverse order.
      const target = flag === causalGraph.DiffFlag.B ? newOps : conflictOps
      // target.push(span)

      let last
      if (target.length > 0 && (last = target[target.length - 1])[0] === span[1]) {
        last[0] = span[0]
      } else {
        target.push(span)
      }
  })
  // newOps and conflictOps will be filled in in reverse order. Fix!
  newOps.reverse(); conflictOps.reverse()

  const ctx: EditContext = {
    items: [],
    delTargets: new Array(oplog.ops.length).fill(-1),
    itemsByLV: new Array(oplog.ops.length).fill(null),
  }

  // We need some placeholder items to correspond to the document as it looked at the commonAncestor state.
  // The placeholderLength needs to be at least the size that the document was at the time. This is inefficient but simple.
  // let conflictOpLen = conflictOps.reduce((sum, [start, end]) => sum + end - start, 0)
  // const placeholderLength = branch.data.length + conflictOpLen
  const placeholderLength = Math.max(...branch.version) + 1
  // const placeholderLength = Math.max(...commonAncestor)
  // Also we must not use IDs that will show up in the actual document.
  // assert(placeholderLength <= Math.min(...commonAncestor))

  for (let i = 0; i < placeholderLength; i++) {
    const opId = i + 1e12
    const item: Item = {
      // TODO: Consider using some weird IDs here instead of normal numbers to make it clear.
      // Right now these IDs are also used in ctx.itemsByLV, but if that becomes a Map instead it would work better.
      opId,
      curState: ItemState.Inserted,
      endState: ItemState.Inserted,
      originLeft: -1,
      rightParent: -1,
    }
    ctx.items.push(item)
    ctx.itemsByLV[opId] = item
  }

  let ctxVersion = commonAncestor
  for (const [start, end] of conflictOps) {
    // console.log('conflict', start, end)
    // While processing the conflicting ops, we don't pass the document state because we don't
    // want the document to be modified yet. We're just building up the items in ctx.
    ctxVersion = walkBetween2(ctx, oplog, ctxVersion, start, end, null)
  }

  for (const [start, end] of newOps) {
    // console.log('newOps', start, end)
    // And now we update the branch.
    ctxVersion = walkBetween2(ctx, oplog, ctxVersion, start, end, branch.data)
  }

  // Set the branch version to the union of the versions.
  // We can't use ctxVersion since it will probably just be the last version we visited.
  branch.version = causalGraph.findDominators(oplog.cg, [...branch.version, ...mergeVersion])
}

export function mergeString(oplog: ListOpLog<string>): string {
  return checkoutSimple(oplog).data.join('')
}

// This implements the core algorithm in as readable a form as I can manage.
//
// Its fully featured, but its missing "standard" performance optimizations I'd
// want in a real library:
//
// - There's no run-length encoding (well, outside the causal-graph library).
//   All operations are split into individual insert / deletes.
// - When we replay operations, *all* operations are processed in full. A
//   better library will skip updating the CRDT when operations are fully ordered.
// - The causal graph is traversed naively

// I say there's no run-length encoding. But because I'm a bit lazy, I'm reusing
// the fancier causal graph implementation that I've made for other projects. This
// CG implementation internally run-length encodes the causal graph in memory.
//
// The causal graph library is used for its graph manipulation helper functions -
// like diff and iterVersionsBetween.
import { CausalGraph, DiffFlag, Id, intersectWithSummary, LV, LVRange } from './causal-graph.js'
import { cloneCursor, ContentCursor, ContentTree, ctCreate } from './content-tree.js'
import { CRDTItem, createPlaceholderItem, ITEM_FUNCS, itemLen, ItemState, itemTakesUpCurSpace, itemTakesUpEndSpace } from './crdtitem.js'
import { IndexTree, MAX_BOUND } from './index-tree.js'
import { Marker, MARKER_FUNCS } from './marker.js'
import { LeafIdx } from './tree-common.js'
import {assert, assertEq, assertNe, max2, min2, pushRLEList} from './utils.js'
import bs from 'binary-search'

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
export type ListOp<T = any> = {
  type: 'ins',
  pos: number
  content: T[], // TODO: Move this to a master "inserted content" array in ListOpLog.
} | {
  type: 'del',
  pos: number,
  len: number, // Number of deleted items.
  // TODO: Fwd / backward.
}

type OpWithVersion<T> = ListOp<T> & { version: LV }

export interface ListOpLog<T = any> {
  // The LV for each op is its index in this list.
  ops: OpWithVersion<T>[],
  cg: CausalGraph,
  // len: number, // The total length of the stored ops array.
}

export function createOpLog<T = any>(): ListOpLog<T> {
  return {
    ops: [],

    // The causal graph stores the IDs (agent,seq) and parents for each
    // of the operations.
    cg: new CausalGraph(),
  }
}

export const opLen = (op: ListOp<any>): number => (
  op.type === 'ins'
    ? op.content.length
    : op.len
)

const findOpIndex = <T>(oplog: ListOpLog<T>, lv: number): number => (
  // Its a pity this needs to call opLen internally. Should be fast - but still.
  bs(oplog.ops, lv, (op, needle) => (
    needle < op.version ? 1
      : needle >= op.version + opLen(op) ? -1
      : 0
  ))
)

/** Iterate through all operations in the range from start to end. Yields the operations only. */
function* oplogIterRange<T>(oplog: ListOpLog<T>, start: LV, end: LV): Generator<OpWithVersion<T>> {
  let idx = findOpIndex(oplog, start)

  // The oplog is packed, so this should only happen when start >= oplog.end.
  if (idx < 0) return

  for (; idx < oplog.ops.length; idx++) {
    const op = oplog.ops[idx]
    if (op.version >= end) break

    let len = opLen(op)

    // At this point, the op should intersect our range somehow.
    assert(op.version < end)
    assert(op.version + len >= start)

    if (op.version >= start && op.version + len <= end) {
      yield op
    } else {
      // Clone the operation, trim it down and yield the trimmed version.
      let op2: OpWithVersion<T> = { ...op }

      let sliceStart = op.version < start
        ? start - op.version
        : 0

      let sliceEnd = op.version + len > end
        ? end - op.version
        : len

      op2.version += sliceStart

      if (op2.type === 'del') {
        op2.len = sliceEnd - sliceStart
      } else {
        op2.pos += sliceStart
        op2.content = op2.content.slice(sliceStart, sliceEnd)
      }

      yield op2
    }
  }
}

function dbgCheckOplog<T>(oplog: ListOpLog<T>) {
  oplog.cg.dbgCheck()

  let actualLen = 0
  for (let op of oplog.ops) {
    actualLen += opLen(op)
  }
  // assertEq(actualLen, oplog.len)

  // We don't necessarily have to assign LVs from zero - but because we do, the
  // "next LV" also counts the number of items in the causal graph.
  // assertEq(oplog.len, oplog.cg.nextLV())
}

function tryMergeOpWithV<T>(a: OpWithVersion<T>, b: OpWithVersion<T>): boolean {
  if (a.type === 'ins' && b.type === 'ins') {
    let len = a.content.length
    // Checking the version probably isn't relevant here because of how I'm using this function.
    // But its still good practice, I think. Dunno how this function will be used later.
    if (a.pos + len === b.pos && a.version + len === b.version) {
      a.content.push(...b.content)
      return true
    }
  } else if (a.type === 'del' && b.type === 'del') {
    // TODO: Also add backspace optimisation.
    if (a.pos === b.pos && a.version + a.len === b.version) {
      a.len += b.len
      return true
    }
  }
  return false
}

export function localInsert<T>(oplog: ListOpLog<T>, agent: string, pos: number, ...content: T[]) {
  const seq = oplog.cg.nextSeqForAgent(agent)
  const version = oplog.cg.nextLV()

  // add returns the number of items missing from oplog.cg.
  let lenAdded = oplog.cg.add(agent, seq, seq + content.length)
  assertEq(lenAdded, content.length)
  pushRLEList(tryMergeOpWithV, oplog.ops, { type: 'ins', pos, content, version })
}

export function localDelete<T>(oplog: ListOpLog<T>, agent: string, pos: number, len: number = 1) {
  if (len === 0) throw Error('Invalid delete length')

  const seq = oplog.cg.nextSeqForAgent(agent)
  const version = oplog.cg.nextLV()

  let lenAdded = oplog.cg.add(agent, seq, seq+len)
  assertEq(lenAdded, len)
  pushRLEList(tryMergeOpWithV, oplog.ops, { type: 'del', pos, len, version })
}

/**
 * Add an operation to the oplog. This is for "remote" operations from other peers
 * or to load operations from a file.
 *
 * Content is required if the operation is an insert.
 *
 * Returns the inserted length.
 */
export function pushRemoteOp<T>(oplog: ListOpLog<T>, id: Id, parents: Id[], op: ListOp<T>): number {
  let len = opLen(op)
  assert(len > 0)

  const version = oplog.cg.nextLV() // Must be called before addRemote.
  let lenAdded = oplog.cg.addRemote(id, len, parents)
  if (lenAdded === 0) return 0 // We already have this operation.

  // if (type === 'ins' && content === undefined) throw Error('Cannot add an insert operation with no content')

  if (len > lenAdded) {
    // Truncate the operation, keeping the tail.
    let sliceAt = len - lenAdded
    if (op.type === 'del') {
      op.len -= sliceAt
    } else {
      op.pos += sliceAt
      op.content = op.content.slice(sliceAt)
    }
  }

  DBG: {
    if (oplog.ops.length > 0) {
      const last = oplog.ops[oplog.ops.length - 1]
      assertEq(last.version + opLen(last), version)
    }
  }

  // Adding the version here constructs a new object.
  const opWithV: OpWithVersion<T> = {
    ...op,
    version
  }

  pushRLEList(tryMergeOpWithV, oplog.ops, opWithV)
  return lenAdded
}

export function getLatestVersion<T>(oplog: ListOpLog<T>): Id[] {
  return oplog.cg.lvToIdList(oplog.cg.heads())
}

/**
 * This function adds everything in the src oplog to dest.
 */
export function mergeOplogInto<T>(dest: ListOpLog<T>, src: ListOpLog<T>) {
  // It would also be possible (and much easier) to convert all operations in
  // src into "remote operations" (using an external ID and such). But this
  // function exactly mirrors how you'd sync peers over the network. I also
  // think (hope) it should be a bit faster. So I'm doing it this way.

  let vs = dest.cg.summarizeVersion()
  const [commonVersion, _remainder] = intersectWithSummary(src.cg.inner, vs)
  // `remainder` lists items in dest that are not in src. Not relevant!

  // Now we need to get all the versions since commonVersion.
  const ranges = src.cg.diff(commonVersion, src.cg.heads()).bOnly

  // Copy the missing CG entries from src to dest.
  const cgDiff = src.cg.serializeDiff(ranges)
  dest.cg.mergePartialVersions(cgDiff)

  // And copy the corresponding operations from the oplog.
  let lv = dest.cg.nextLV()
  for (const [start, end] of ranges) {
    for (const srcOp of oplogIterRange(src, start, end)) {
      // Now, I could use pushRemoteOp here but we've already updated the
      // destination CG directly.
      const destOp = { ...srcOp }
      destOp.version = lv
      pushRLEList(tryMergeOpWithV, dest.ops, destOp)

      let len = opLen(destOp)
      lv += len
    }
  }
}


// *** Merging changes ***

interface EditContext {
  /**
   * All the items in document order. This list is grow-only, and will be spliced()
   * in as needed.
   */
  items: ContentTree<CRDTItem>,

  /**
  * The index stores two pieces of information:
  *
  * - For inserts, it stores the corresponding leaf index in items of that particular
  *   insert.
  * - For deletes, it stores the delete's target.
  */
  index: IndexTree<Marker>,

  /**
   * Items in the EditContext have 2 version tags - curState and endState. These
   * store the state at 2 different versions while we traverse the operations.
   *
   * This parameter stores the current version itself.
   */
  curVersion: number[],
}

function markerAt(ctx: EditContext, lv: LV): LeafIdx {
  let marker = ctx.index.getEntry(lv).val
  if (marker.type !== 'ins') throw Error('No marker at lv')
  return marker.leaf
}

function advRetreatRange(ctx: EditContext, lvStart: LV, lvEnd: LV, isAdvance: boolean) {
  // This does advance / retreat of a whole range from lvStart to lvEnd.
  //
  // In theory, retreats should happen in reverse order (latest to earliest). But
  // Because we're only incrementing and decrementing the state of various items
  // in the tree, the order that visit items is mathematically irrelevant.
  // So we'll just go in order, in all cases to keep the code simpler.

  const incr = isAdvance ? 1 : -1

  while (lvStart < lvEnd) {
    // This is an optimisation. Try and just edit the inserted item at the cached
    // cursor.
    let cursor = ctx.items.tryFindItemAtCursor(lvStart)
    if (cursor) {
      // This makes things much faster. Note this will only happen for inserts -
      // since deleted items aren't in the range tree.
      let e = ctx.items.getItem(cursor)
      let start = max2(lvStart, e.lvStart)
      cursor.offset = start - e.lvStart
      let maxLen = lvEnd - start

      lvStart += ctx.items.mutateEntry(cursor, maxLen, e => { e.curState += incr })

    } else {
      // This is the normal case.
      let { start: entryStart, end: entryEnd, val: marker } = ctx.index.getEntry(lvStart)
      // console.log('looking up', lvStart, {entryStart, entryEnd, marker})
      const len = min2(entryEnd, lvEnd) - lvStart

      let targetStart, leaf = MAX_BOUND

      if (marker.type === 'ins') {
        leaf = marker.leaf
        assertNe(leaf, MAX_BOUND, 'Item not found in content')
        // We'll just modify from start to end in the inserted item itself.
        targetStart = lvStart
      } else {
        // For deletes, we modify the *target* of the delete.
        let offset = lvStart - entryStart
        targetStart = marker.fwd
          ? marker.target + offset
          : marker.target - offset - len
      }

      const targetEnd = targetStart + len
      while (targetEnd > targetStart) {
        let leaf_here = leaf !== MAX_BOUND ? leaf : markerAt(ctx, targetStart)

        // We can't reuse the leaf ptr across subsequent invocations because we mutate the range
        // tree. As such, the leaf index we requested earlier is invalid.
        leaf = MAX_BOUND

        let cursor = ctx.items.cachedCursorBeforeItem(targetStart, leaf_here)
        targetStart += ctx.items.mutateEntry(cursor, targetEnd - targetStart, e => {
          // Actually modify the element.
          e.curState += incr
        })

        ctx.items.emplaceCursorUnknown(cursor)
      }

      lvStart += len
    }
  }
}

function applyRange<T>(ctx: EditContext, snapshot: T[] | null, oplog: ListOpLog<T>, start: LV, end: LV) {
  if (start === end) return

  // console.log('start', start, 'end', end)
  for (let op of oplogIterRange(oplog, start, end)) {
    let start = op.version
    let len = opLen(op)
    let end = start + len
    let cloned = false

    // console.log('applying op', len, 'v range', op.version, op.version + len, 'op', op)

    // The operations may cross boundaries between users. We need to split them up along user agent
    // bounds.
    for (const cgEntry of oplog.cg.iterVersionsBetween(start, end)) {
      assertEq(cgEntry.version, start)
      assert(cgEntry.vEnd <= end)

      while (len > 0) {
        const [lenHere, xfPos] = applyXF(ctx, op, oplog.cg, cgEntry.agent, cgEntry.seq)

        if (xfPos >= 0) {
          // Apply the operation to the snapshot.
          if (op.type === 'ins') {
            snapshot?.splice(xfPos, 0, ...op.content)
            // console.log('INS', xfPos, op.content, '->', (snapshot as string[]).join(''))
          } else {
            const deleted = snapshot?.splice(xfPos, lenHere)
            // console.log('DEL', lenHere, `'${(deleted as string[]).join('')}' -> '${(snapshot as string[]).join('')}'`)
          }
        }

        len -= lenHere
        if (len !== 0) {
          // This is really gross, and I think I could mostly do without it.
          // I might just have to hoist the version, position and del len.
          // Note that applyXF will only split operations when they're deletes.
          if (!cloned) {
            op = { ...op }
            cloned = true
          }

          op.version += lenHere
          if (op.type === 'ins') {
            // This won't actually happen, because applyXF only splits deletes...
            op.pos += lenHere
            op.content = op.content.slice(lenHere)
          } else {
            op.len -= lenHere
          }
        }
      }

      start = cgEntry.vEnd
    }
  }
}

// -1 to indicate the delete already happened - and discard the operation.
type TransformedPosition = number | -1

// Apply the start of an operation to the internal editing content, returning the transformed
// version of the operation.
//
// This function may not apply the entire operation. This only happens if the operation is a delete
// and the delete is split by inserts or regions which have already been deleted. In that case, this
// method must be called in a loop.
//
// Returns the amount of the op we've processed, and the transformed position (ie, where it goes in the output).
function applyXF(ctx: EditContext, op: OpWithVersion<any>, cg: CausalGraph, agent: string, seq: number): [number, TransformedPosition] {
  let len = opLen(op)

  if (op.type === 'ins') {
    let originLeft = -1

    let curPos = op.pos
    let endPos = 0

    let cursor: ContentCursor

    if (op.pos === 0) {
      cursor = ctx.items.cursorAtStart()
    } else {
      // We need to read out the LV of the previous item to calculate originLeft.
      // We'll get a cursor at op.pos - 1, read the LV there then advance the cursor
      // to our insert position.
      [endPos, cursor] = ctx.items.cursorBeforeCurPos(op.pos - 1)
      const e = ctx.items.getItem(cursor)
      originLeft = e.lvStart + cursor.offset
      endPos += e.endStateEverDeleted ? 0 : 1
      cursor.offset++
    }

    let originRight
    // Advance the cursor to the next item. Because placeholders exist, this should always
    // succeed.
    if (!ctx.items.cursorRollNextItem(cursor)) throw Error('Could not roll next item')

    // We'll make another cursor and scan forward to find the origin right.
    // origin right is the next item in the list which exists at this point in time.
    let c2 = cloneCursor(cursor)
    while (true) {
      let e = ctx.items.getItem(c2)
      if (e.curState !== ItemState.NotYetInserted) {
        originRight = e.lvStart + cursor.offset
        break
      }
      // Again, because of placeholder items we will always find something.
      // If there were no placeholder items, if we run out of content, originRight should
      // just be set to -1.
      if (!ctx.items.cursorNextEntry(c2)) throw Error('Could not find right item')
    }

    const item: CRDTItem = {
      lvStart: op.version,
      lvEnd: op.version + len,
      originLeft,
      originRight,
      curState: ItemState.Inserted,
      endStateEverDeleted: false,
    }

    // console.log('inserting item', item)
    const xfPos = integrate(ctx, cg, item, agent, seq, cursor, curPos, endPos)
    return [len, xfPos] // Inserts are always processed in their entirity.
  } else { // Deletes!
    // Delete as much as we can. We might not be able to delete everything because of
    // double deletes and inserts inside the deleted range. This is extra annoying
    // because we need to move backwards through the deleted items if we're rev.
    assert(len > 0)
    const fwd = true // TODO.

    let curPos: number, endPos: number, cursor: ContentCursor

    if (fwd) {
      curPos = op.pos
      ;[endPos, cursor] = ctx.items.cursorBeforeCurPos(curPos)
    } else {
      throw Error('Not implemented')
    }

    let e = ctx.items.getItem(cursor)
    assertEq(e.curState, ItemState.Inserted)

    // If this item has never been deleted, its time.
    const everDeleted = e.endStateEverDeleted

    const [len2, targetStart] = ctx.items.mutateEntry2(cursor, len, e => {
      e.curState++
      e.endStateEverDeleted = true
      return e.lvStart
    })
    const targetEnd = targetStart + len2

    // The cursor shouldn't have moved, since the item we traversed over was
    // deleted.
    ctx.items.emplaceCursor(curPos, endPos, cursor)

    len = len2

    let lvStart = op.version
    ctx.index.setRange(lvStart, lvStart + len, {
      type: 'del',
      fwd,
      target: fwd ? targetStart : targetEnd
    })

    // console.log('del xf', op.pos, '->', endPos)
    return [len, everDeleted ? -1 : endPos]
  }
}

function getCursorBefore(ctx: EditContext, lv: LV): ContentCursor {
  if (lv < 0 || lv === MAX_BOUND) throw Error('Invalid LV')
  let leaf = markerAt(ctx, lv)
  return ctx.items.cursorBeforeItem(lv, leaf)
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
function integrate(
  ctx: EditContext, cg: CausalGraph,
  item: CRDTItem, agent: string, seq: number,
  cursor: ContentCursor, curCur: number, curEnd: number)
{
  // TODO: Cloning objects in javascript can be expensive. Move these close statements
  // underneath the initial loop break.
  const leftCursor = cloneCursor(cursor)
  let scanCursor = cloneCursor(cursor)
  let scanCur = curCur, scanEnd = curEnd
  let scanning = false

  const items = ctx.items

  // I think rollNextItem can't ever return false because of placeholder items..?
  // If cursor.offset != 0, we must be inserting in the middle of an already inserted
  // item - in which case we don't need to do any of this.
  //
  // Probably don't need to check it each iteration?
  while (cursor.offset === 0 && items.cursorRollNextItem(cursor)) {
    const otherEntry = items.getItem(cursor)

    // When concurrent edits happen, the range of insert locations goes from the insert
    // position itself (passed in through cursor) to the next item which existed at the
    // time in which the insert occurred.
    let otherLv = otherEntry.lvStart
    // This test is almost always true. (Ie, we basically always break here).
    if (otherLv == item.originRight) break

    // TODO: Is this necessary? Once fuzz tests pass, try commenting this line out.
    // items.flushDelta()

    assertEq(otherEntry.curState, ItemState.NotYetInserted)

    // This code could be better optimized, but its already O(n * log n), and its extremely
    // rare that you actually get concurrent inserts at the same location in the document
    // anyway.
    //
    let otherLeftLV = cursor.offset === 0
      ? otherEntry.originLeft
      : otherEntry.lvStart + cursor.offset - 1

    if (otherLeftLV === item.originLeft) {
      // Tie break by looking at origin right.
      if (otherEntry.originRight === item.originRight) {
        // So much for that. Items are concurrent. Order by agent / seq.
        let [otherAgent, otherSeq] = cg.lvToId(otherLv)
        let insHere = agent === otherAgent
          ? seq < otherSeq
          : agent < otherAgent
        if (insHere) break
        else scanning = false
      } else {
        let myRightCursor = getCursorBefore(ctx, item.originRight)
        let otherRightCursor = getCursorBefore(ctx, otherEntry.originRight)

        // Set scanning based on how the origin right entries are sorted.
        if (items.compareCursors(otherRightCursor, myRightCursor) < 0) {
          if (!scanning) {
            scanning = true
            scanCursor = cloneCursor(cursor)
            scanCur = curCur
            scanEnd = curEnd
          }
        } else {
          scanning = false
        }
      }
    } else {
      // Compare the position of the origin left items. I'll create a cursor to the
      // other items' origin left, and then compare the cursors in the content tree.
      let otherLeftCursor
      if (otherLeftLV === -1) otherLeftCursor = items.cursorAtStart()
      else {
        // Create a cursor *after* the other item's origin left.
        otherLeftCursor = getCursorBefore(ctx, otherLeftLV)
        otherLeftCursor.offset++
        items.cursorRollNextItem(otherLeftCursor)
      }

      if (items.compareCursors(otherLeftCursor, leftCursor) < 0) break
    }

    // We can skip immediately to comparing the next entry in the content tree.
    // This is quite surprising - but the fuzzer backs me up that doing this has no
    // effect on the resulting document order.
    curCur += itemTakesUpCurSpace(otherEntry) ? itemLen(otherEntry) : 0
    curEnd += itemTakesUpEndSpace(otherEntry) ? itemLen(otherEntry) : 0

    // We could do something sensible if we reach the end of the list, but placeholder data means
    // we never will.
    if (!items.cursorNextEntry(cursor)) throw Error('Invalid - reached end of content tree')
  }

  if (scanning) {
    cursor = scanCursor
    curCur = scanCur
    curEnd = scanEnd
  }

  // Finally we can insert!
  const len = itemLen(item)
  assert(itemTakesUpCurSpace(item))
  assert(itemTakesUpEndSpace(item))

  items.insert(item, cursor)

  // The cursor position needs to be incremented by the length of item.
  // (We're cheating a little here since we know the item will always be in the inserted state)
  items.emplaceCursor(curCur + len, curEnd + len, cursor)

  // The resulting inserted position in the snapshot.
  return curEnd
}

// // This is a helper debugging function, for printing out the internal state of the
// // editing context.
// function debugPrintCtx<T>(ctx: EditContext, oplog: ListOpLog<T>) {
//   console.log('---- DT STATE ----')

//   const depth: Record<number, number> = {}
//   depth[-1] = 0

//   for (const item of ctx.items) {
//     const isLeftChild = true
//     const parent = isLeftChild ? item.originLeft : item.originRight
//     const d = parent === -1 ? 0 : depth[parent] + 1

//     depth[item.lv] = d
//     const lvToStr = (lv: number) => {
//       if (lv === -1) return 'ROOT'
//       const rv = causalGraph.lvToRaw(oplog.cg, lv)
//       return `[${rv[0]},${rv[1]}]`
//     }

//     const op = oplog.ops[item.lv]
//     if (op.type !== 'ins') throw Error('Invalid state') // This avoids a typescript type error.
//     const value = item.endState === ItemState.Deleted ? null : op.content

//     let content = `${value == null ? '.' : value} at ${lvToStr(item.lv)} (left ${lvToStr(item.originLeft)})`
//     content += ` right ${lvToStr(item.originRight)}`
//     console.log(`${'| '.repeat(d)}${content}`)
//   }
// }

/**
 * Traverse and apply the operations in the oplog.
 *
 * This function runs the core merging logic, traversing the
 * graph of changes and modifying 2 structures along the way:
 * - The ctx.items will have fugue style items inserted, and their state
 *   changed.
 * - The passed in data array (document snapshot) will be modified.
 *
 * @param ctx The (in memory) editing context with fugue items at some state
 * @param oplog The log of operations we're applying
 * @param snapshot The document snapshot to modify. When the function returns,
 * this contains the final document state.
 * @param fromOp The index of the first operation to traverse over
 * @param toOp The bound on the indexes to traverse over
 */
export function traverseAndApply<T>(
  ctx: EditContext,
  oplog: ListOpLog<T>,
  snapshot: T[] | null,
  fromOp: number = 0,
  toOp: number = oplog.cg.nextLV() // Same as oplog.ops.length.
) {
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

  for (const entry of oplog.cg.iterVersionsBetween(fromOp, toOp)) {
    const {aOnly, bOnly} = oplog.cg.diff(ctx.curVersion, entry.parents)

    // The causal graph library run-length encodes everything.
    // These are all ranges of operations.
    const retreat = aOnly
    const advance = bOnly

    // Operations to apply.
    const consumeStart = entry.version
    const consumeEnd = entry.vEnd

    // console.log('retreat', retreat, 'advance', advance, 'consume', [consumeStart, consumeEnd])

    // Retreat.
    for (const [start, end] of retreat) {
      advRetreatRange(ctx, start, end, false)
    }

    // Advance.
    for (const [start, end] of advance) {
      advRetreatRange(ctx, start, end, true)
    }

    // Then apply the operation.
    applyRange(ctx, snapshot, oplog, consumeStart, consumeEnd)

    // After processing these operations, we're at the last version in the range.
    ctx.curVersion.length = 1
    ctx.curVersion[0] = entry.vEnd - 1
  }
}

export interface Branch<T = any> {
  snapshot: T[],
  version: number[]
}

export function createEmptyBranch(): Branch<any> {
  return { snapshot: [], version: [] }
}

function createEditContext(curVersion: LV[] = []): EditContext {
  const index = new IndexTree(MARKER_FUNCS)
  const items = new ContentTree(ITEM_FUNCS, (e, leaf) => {
    // console.log('notify', e, leaf)
    index.setRange(e.lvStart, e.lvEnd, {type: 'ins', leaf})
  })

  // The items list is initialized with a single placeholder item.
  items.setSingleItem(createPlaceholderItem())

  return { items, index, curVersion }
}

export function checkout<T>(oplog: ListOpLog<T>): Branch<T> {
  let ctx = createEditContext()

  // The resulting document snapshot
  const snapshot: T[] = []
  traverseAndApply(ctx, oplog, snapshot)

  return {
    snapshot,
    version: oplog.cg.heads().slice()
  }
}

export function checkoutSimple<T>(oplog: ListOpLog<T>): T[] {
  return checkout(oplog).snapshot
}

export function checkoutSimpleString(oplog: ListOpLog<string>): string {
  return checkoutSimple(oplog).join('')
}


// *** FANCY MERGING ***

// The above checkout code will work when we want to process all operations
// into a new document snapshot. This code will allow a branch (a snapshot
// at some version) to be updated with new changes.


export function mergeChangesIntoBranch<T>(branch: Branch<T>, oplog: ListOpLog<T>, mergeVersion: number[] = oplog.cg.heads()) {
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

  let commonAncestor = oplog.cg.findConflicting(branch.version, mergeVersion, (span, flag) => {
      // Note this visitor function visits these operations in reverse order.
      const target = flag === DiffFlag.B ? newOps : conflictOps
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

  const ctx = createEditContext()

  for (const [start, end] of conflictOps) {
    // console.log('conflict', start, end)
    // While processing the conflicting ops, we don't pass the document state because we don't
    // want the document to be modified yet. We're just building up the items in ctx.
    traverseAndApply(ctx, oplog, null, start, end)
  }

  for (const [start, end] of newOps) {
    // console.log('newOps', start, end)
    // And now we update the branch.
    // ctxVersion = walkBetween2(ctx, oplog, ctxVersion, start, end, branch.data)
    traverseAndApply(ctx, oplog, branch.snapshot, start, end)
  }

  // Set the branch version to the union of the versions.
  // We can't use ctxVersion since it will probably just be the last version we visited.
  branch.version = oplog.cg.findDominators([...branch.version, ...mergeVersion])
}



// --------

// const oplog = createOpLog()
// pushRemoteOp(oplog, ['a', 0], [], { type: 'ins', pos: 0, content: [0] })
// pushRemoteOp(oplog, ['b', 0], [], { type: 'ins', pos: 0, content: [1] })
// // console.log('oplog', oplog)
// console.log(checkoutSimple(oplog))

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
import * as causalGraph from "./causal-graph.js"

// ** A couple utility methods **
function assert(expr: boolean, msg?: string) {
  if (!expr) throw Error(msg != null ? `Assertion failed: ${msg}` : 'Assertion failed')
}

function assertEq<T>(a: T, b: T, msg?: string) {
  if (a !== b) throw Error(`Assertion failed: ${a} !== ${b} ${msg ?? ''}`)
}

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
  content: T
} | {
  type: 'del',
  pos: number,
}

export interface ListOpLog<T = any> {
  // The LV for each op is its index in this list.
  ops: ListOp<T>[],
  cg: causalGraph.CausalGraph,
}

export function createOpLog<T = any>(): ListOpLog<T> {
  return {
    ops: [],

    // The causal graph stores the IDs (agent,seq) and parents for each
    // of the operations.
    cg: causalGraph.createCG()
  }
}

export function localInsert<T>(oplog: ListOpLog<T>, agent: string, pos: number, ...content: T[]) {
  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+content.length, oplog.cg.heads)
  for (const val of content) {
    oplog.ops.push({ type: 'ins', pos, content: val })
    pos++ // Each successive insert happens at the next location.
  }
}

export function localDelete<T>(oplog: ListOpLog<T>, agent: string, pos: number, len: number = 1) {
  if (len === 0) throw Error('Invalid delete length')

  const seq = causalGraph.nextSeqForAgent(oplog.cg, agent)
  causalGraph.add(oplog.cg, agent, seq, seq+len, oplog.cg.heads)
  for (let i = 0; i < len; i++) {
    oplog.ops.push({ type: 'del', pos })
  }
}

/** Add an operation to the oplog. Content is required if the operation is an insert. */
export function pushOp<T>(oplog: ListOpLog<T>, id: causalGraph.RawVersion, parents: causalGraph.RawVersion[], type: 'ins' | 'del', pos: number, content?: T): boolean {
  const entry = causalGraph.addRaw(oplog.cg, id, 1, parents)
  if (entry == null) return false // We already have this operation.

  if (type === 'ins' && content === undefined) throw Error('Cannot add an insert operation with no content')
  assertEq(entry.version, oplog.ops.length, 'Invalid state: oplog length and cg do not match')

  const op: ListOp<T> = type === 'ins' ? {
    type, pos, content: content!
  } : {
    type, pos
  }

  oplog.ops.push(op)
  return true
}

export function getLatestVersion<T>(oplog: ListOpLog<T>): causalGraph.RawVersion[] {
  return causalGraph.lvToRawList(oplog.cg, oplog.cg.heads)
}

/**
 * This function adds everything in the src oplog to dest.
 */
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


// *** Merging changes ***

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
  /**
   * All the items in document order. This list is grow-only, and will be spliced()
   * in as needed.
   */
  items: Item[],

  /**
   * When we delete something, we store the LV of the item that was deleted. This is
   * used when items are un-deleted (and re-deleted).
   * delTarget[del_lv] = target_lv.
   */
  delTargets: number[],

  /**
   * This is the same set of items as above, but this time indexed by LV. This is
   * used to make it fast & easy to activate and deactivate items.
   */
  itemsByLV: Item[],

  /**
   * Items in the EditContext have 2 version tags - curState and endState. These
   * store the state at 2 different versions while we traverse the operations.
   *
   * This parameter stores the current version itself.
   */
  curVersion: number[],
}

function advance1<T>(ctx: EditContext, oplog: ListOpLog<T>, opId: number) {
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

function retreat1<T>(ctx: EditContext, oplog: ListOpLog<T>, opId: number) {
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

function apply1<T>(ctx: EditContext, snapshot: T[] | null, oplog: ListOpLog<T>, opId: number) {
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
      if (snapshot) snapshot.splice(cursor.endPos, 1)
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
    if (snapshot) snapshot.splice(cursor.endPos, 0, op.content!)
  }
}

// This is a helper debugging function, for printing out the internal state of the
// editing context.
function debugPrintCtx<T>(ctx: EditContext, oplog: ListOpLog<T>) {
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
  toOp: number = causalGraph.nextLV(oplog.cg) // Same as oplog.ops.length.
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
  for (const entry of causalGraph.iterVersionsBetween(oplog.cg, fromOp, toOp)) {
    const {aOnly, bOnly} = causalGraph.diff(oplog.cg, ctx.curVersion, entry.parents)

    // The causal graph library run-length encodes everything.
    // These are all ranges of operations.
    const retreat = aOnly
    const advance = bOnly
    const consume = [entry.version, entry.vEnd] // Operations to apply.

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
      apply1(ctx, snapshot, oplog, lv)
    }

    // After processing these operations, we're at the last version in the range.
    ctx.curVersion = [entry.vEnd - 1]
  }
}

export interface Branch<T = any> {
  snapshot: T[],
  version: number[]
}

export function createEmptyBranch(): Branch<any> {
  return { snapshot: [], version: [] }
}

export function checkout<T>(oplog: ListOpLog<T>): Branch<T> {
  const ctx: EditContext = {
    items: [],
    delTargets: new Array(oplog.ops.length).fill(-1),
    itemsByLV: new Array(oplog.ops.length).fill(null),
    curVersion: [],
  }

  // The resulting document snapshot
  const snapshot: T[] = []
  traverseAndApply(ctx, oplog, snapshot)

  return {
    snapshot,
    version: oplog.cg.heads.slice()
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
  const newOps: causalGraph.LVRange[] = []
  const conflictOps: causalGraph.LVRange[] = []

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
    curVersion: commonAncestor,
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

  // let ctxVersion = commonAncestor
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
  branch.version = causalGraph.findDominators(oplog.cg, [...branch.version, ...mergeVersion])
}

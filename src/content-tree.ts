import { assert, assertEq, assertNe } from './utils.js'
import {
  NODE_CHILDREN,
  LEAF_CHILDREN,
  NODE_SPLIT_POINT,
  LEAF_SPLIT_POINT,
  LV,
  LeafIdx,
  NodeIdx,
  NULL_IDX,
} from './tree-common.js'
import { MAX_BOUND } from './index-tree.js'

export type NotifyFn<V> = (val: V, leaf: LeafIdx) => void

export class ContentTree<V> {
  inner: ContentTreeInner<V>
  constructor(funcs: ContentTreeFuncs<V>, notify: NotifyFn<V>) {
    this.inner = ctCreate(funcs, notify)
  }

  setSingleItem(item: V) {
    ctSetSingleItem(this.inner, item)
  }

  /** Try and find the requested item at the cached cursor */
  tryFindItemAtCursor(lv: LV) {
    return ct_try_find_item_at_cursor(this.inner, lv)
  }

  getItem(cursor: ContentCursor) {
    return ct_get_item(this.inner, cursor)
  }

  /**
   * Mutate the next item under the cursor, with maximum size of replace_max.
   * Map function is expected to mutate the item in-place.
   *
   * Mutation function must not modify the item's length.
   */
  mutateEntry(cursor: ContentCursor, replace_max: number, map_fn: (v: V) => void): number {
    return ct_mutate_entry(this.inner, cursor, replace_max, map_fn)[0]
  }

  /**
   * Mutate the next item under the cursor, with maximum size of replace_max.
   * Map function is expected to mutate the item in-place.
   *
   * Mutation function must not modify the item's length.
   *
   * This variant allows the map function to return a value. The value is returned.
   */
  mutateEntry2<R>(cursor: ContentCursor, replace_max: number, map_fn: (v: V) => R): [number, R] {
    return ct_mutate_entry(this.inner, cursor, replace_max, map_fn)
  }

  insert(item: V, cursor: ContentCursor) {
    ct_insert(this.inner, item, cursor, true)
  }

  // Cursor methods. Maybe I should wrap the cursor in a class too?
  cursorAtStart(): ContentCursor {
    return ct_cursor_at_start(this.inner)
  }

  cursorBeforeItem(id: LV, leafIdx: LeafIdx): ContentCursor {
    return ct_cursor_before_item(this.inner, id, leafIdx)
  }
  cachedCursorBeforeItem(id: LV, leafIdx: LeafIdx): ContentCursor {
    return ct_cached_cursor_before_item(this.inner, id, leafIdx)
  }

  cursorBeforeCurPos(pos: number) {
    return ct_cursor_before_cur_pos(this.inner, pos)
  }

  emplaceCursor(curPos: number, endPos: number, cursor: ContentCursor) {
    ct_emplace_cursor(this.inner, curPos, endPos, cursor)
  }

  emplaceCursorUnknown(cursor: ContentCursor) {
    ct_emplace_cursor_unknown(this.inner, cursor)
  }

  /** If the cursor is at the end of the current entry, move it to the start of the next entry */
  cursorRollNextItem(cursor: ContentCursor): boolean {
    return cursor_roll_next_item(this.inner, cursor)
  }

  /**
   * Move the cursor to the start of the following entry.
   * Returns true if the cursor lands in an item. Or false if we're at the end of the tree.
   */
  cursorNextEntry(cursor: ContentCursor): boolean {
    return nextEntry(this.inner, cursor)
  }

  compareCursors(a: ContentCursor, b: ContentCursor): number {
    return ct_cmp_cursor(this.inner, a, b)
  }

  dbgPrint() {
    ct_print_tree(this.inner)
  }
}

interface ContentTreeInner<V> {
  leaf_values: V[] // num leaves * LEAF_CHILDREN.
  leaf_next: LeafIdx[] // num leaves
  leaf_parents: NodeIdx[] // num leaves

  node_child_indexes: number[], // num nodes * NODE_CHILDREN. Filled with NULL_IDX for unused child slots.
  node_child_width: number[], // num nodes * NODE_CHILDREN * 2. [cur, end].
  node_parents: NodeIdx[] // num nodes

  /// The number of internal nodes between the root and the leaves. This is initialized to 0,
  /// indicating we start with no internal nodes and just a single leaf.
  height: number,

  /// The root node. If height == 0, this is a leaf (and has value 0). Otherwise, this is an index
  /// into the nodes vec pointing to the node representing the root.
  root: number,

  _cur_len: number,
  _end_len: number,

  funcs: ContentTreeFuncs<V>,

  /// There is a cached cursor currently at some content position, with a held delta update.
  // cursor: Option<(Option<LenPair>, ContentCursor)>,
  cursor: ContentCursor | null,

  // This is the current position of the cursor.
  // TODO: Consider moving these to the cursor.
  cursor_cur: number, // or MAX
  cursor_end: number, // or MAX


  // This is an inlined "delta update". Should be 0 in many cases.
  upd_leaf: LeafIdx, // or MAX_BOUND.
  upd_cur: number,
  upd_end: number,

  notify: NotifyFn<V>,
}

export interface ContentTreeFuncs<V> { // : SplitableSpan + MergableSpan
  content_len_cur(val: V): number,
  content_len_end(val: V): number,

  takes_up_space_cur(val: V): boolean,
  takes_up_space_end(val: V): boolean,

  raw_len(val: V): number,

  /// The default item must "not exist".
  exists(val: V): boolean,
  // takes_up_space<const IS_CUR: bool>(&self): bool;

  none(): V, // TODO: This should probably just be a constant.

  // V is truncated. Returns the remainder.
  truncate(val: V, at: number): V,
  truncate_keeping_right(val: V, at: number): V,

  // Returns true if a was mutated to contain the contents of b.
  tryAppend(a: V, b: V): boolean,

  find(val: V, lv: LV): number, // Returns offset if found. Otherwise -1.
}

interface Delta {
  cur: number,
  end: number,
}

export interface ContentCursor {
  leaf_idx: LeafIdx,
  elem_idx: number,

  // Offset into the item.
  offset: number,
}

export function cloneCursor(cursor: ContentCursor): ContentCursor {
  return {
    elem_idx: cursor.elem_idx,
    leaf_idx: cursor.leaf_idx,
    offset: cursor.offset,
  }
}

function pushLeaf<V>(tree: ContentTreeInner<V>): LeafIdx {
  const newIdx = tree.leaf_next.length
//   const newLen = newIdx + 1

  for (let i = 0; i < LEAF_CHILDREN; i++) {
    tree.leaf_values.push(tree.funcs.none()) // TODO: Consider sharing this item.
  }

  // tree.leaf_values.length = LEAF_CHILDREN * newLen
  // for (let i = 0; i < LEAF_CHILDREN; i++) {
  //   tree.leaf_values[newIdx * LEAF_CHILDREN + i] = tree.funcs.none()
  // }
  // tree.leaf_values.fill( // fill with default()?

  tree.leaf_next.push(NULL_IDX)
  tree.leaf_parents.push(NULL_IDX)

  return newIdx
}

function pushNode<V>(tree: ContentTreeInner<V>): NodeIdx {
  let newIdx = tree.node_parents.length

  tree.node_child_indexes.length = (newIdx + 1) * NODE_CHILDREN
  tree.node_child_indexes.fill(NULL_IDX, newIdx * NODE_CHILDREN, tree.node_child_indexes.length)

  tree.node_child_width.length = (newIdx + 1) * NODE_CHILDREN * 2
  tree.node_child_width.fill(0, newIdx * NODE_CHILDREN * 2, tree.node_child_width.length)

  tree.node_parents.push(NULL_IDX)
  return newIdx
}

function leaf_has_space<V>(tree: ContentTreeInner<V>, leaf_idx: LeafIdx, space_wanted: number): boolean {
  if (space_wanted === 0) return true // Is this necessary?
  return !tree.funcs.exists(tree.leaf_values[(leaf_idx + 1) * LEAF_CHILDREN - space_wanted]);
}

function leaf_is_last<V>(tree: ContentTreeInner<V>, leaf_idx: LeafIdx): boolean {
  return tree.leaf_next[leaf_idx] != NULL_IDX
}

function node_is_full<V>(tree: ContentTreeInner<V>, node_idx: NodeIdx): boolean {
  return tree.node_child_indexes[(node_idx + 1) * NODE_CHILDREN - 1] != NULL_IDX
}

// TODO: Consider sharing this method with index-tree.
function node_idx_of_child<V>(tree: ContentTreeInner<V>, node_idx: NodeIdx, child: number): number {
  const base = node_idx * NODE_CHILDREN
  let idx = tree.node_child_indexes.indexOf(child, node_idx * NODE_CHILDREN)
  assert(idx >= base && idx < base + NODE_CHILDREN)
  return idx - base
}

function flush_delta<V>(tree: ContentTreeInner<V>) {
  if (tree.upd_cur === 0 && tree.upd_end === 0) return

  let leaf = tree.upd_leaf
  assert(leaf >= 0 && leaf < tree.leaf_next.length)

  let idx = tree.leaf_parents[leaf]
  let child = leaf
  while (idx != NULL_IDX) {
    const pos = node_idx_of_child(tree, idx, child)

    let base = (idx * NODE_CHILDREN + pos) * 2
    tree.node_child_width[base + 0] += tree.upd_cur
    tree.node_child_width[base + 1] += tree.upd_end

    child = idx
    idx = tree.node_parents[idx]
  }

  tree._cur_len += tree.upd_cur
  tree._end_len += tree.upd_end

  tree.upd_cur = tree.upd_end = 0
}

// function flush_delta_and_clear<V>(tree: ContentTree<V>, leaf: LeafIdx, delta: DeltaUpdate) {
//   flush_delta(tree, leaf, delta)
//   delta.upd_cur = delta.upd_end = 0
// }

function set_delta_to(tree: ContentTreeInner<any>, leaf: LeafIdx) {
  if (tree.upd_leaf !== leaf) {
    flush_delta(tree)
    tree.upd_leaf = leaf
  }
}

function inc_delta_update_by<V>(tree: ContentTreeInner<V>, leaf: LeafIdx, e: V) {
  set_delta_to(tree, leaf)
  tree.upd_cur += tree.funcs.content_len_cur(e)
  tree.upd_end += tree.funcs.content_len_end(e)
}

function dec_delta_update_by<V>(tree: ContentTreeInner<V>, leaf: LeafIdx, e: V) {
  set_delta_to(tree, leaf)
  tree.upd_cur -= tree.funcs.content_len_cur(e)
  tree.upd_end -= tree.funcs.content_len_end(e)
}

// *** Cursor functions

const cur_value_offset = (cursor: ContentCursor) => (
  cursor.leaf_idx * LEAF_CHILDREN + cursor.elem_idx
)

/** Returns true if the cursor lands in an item. Or false if we're at the end of the tree. */
function cursor_roll_next_item<V>(tree: ContentTreeInner<V>, cursor: ContentCursor): boolean {
  if (cursor.offset < tree.funcs.raw_len(tree.leaf_values[cur_value_offset(cursor)])) {
    // We're inside an element. Continue.
    return true
  }

  return nextEntry(tree, cursor);
}

/** Returns true if the cursor lands in an item. Or false if we're at the end of the tree. */
function nextEntry<V>(tree: ContentTreeInner<V>, cursor: ContentCursor): boolean {
  cursor.elem_idx++;
  cursor.offset = 0;

  // if (cursor.elem_idx >= LEAF_CHILDREN || !tree.funcs.exists(tree.leaf_values[cur_value_offset(cursor)])) {
  if (cursor.elem_idx < LEAF_CHILDREN && tree.funcs.exists(tree.leaf_values[cur_value_offset(cursor)])) {
    // We're still inside one of the leaf's children.
    return true
  }

  // // Otherwise, flush here and go to the next node.
  // if (cursor.upd_cur || cursor.upd_end) {
  //   flush_delta_and_clear(tree, cursor.leaf_idx, cursor)
  // }

  cursor.leaf_idx = tree.leaf_next[cursor.leaf_idx]
  cursor.elem_idx = 0

  return cursor.leaf_idx != NULL_IDX
}

export function ct_inc_cursor_offset<V>(tree: ContentTreeInner<V>, cursor: ContentCursor) {
  DBG: {
    let e = tree.leaf_values[cur_value_offset(cursor)]
    let len = tree.funcs.raw_len(e)
    assert(cursor.offset < len)
  }

  cursor.offset++
}

export function ct_get_item<V>(tree: ContentTreeInner<V>, cursor: ContentCursor): V {
  return tree.leaf_values[cur_value_offset(cursor)]
}

function assert_cursor_at<V>(tree: ContentTreeInner<V>, cursor: ContentCursor, exp_cur: number, exp_end: number) {
  DBG: {
    let cur = 0, end = 0

    let idx_off = cur_value_offset(cursor)
    let e = tree.leaf_values[idx_off]
    assert(cursor.offset <= tree.funcs.raw_len(e))
    if (tree.funcs.takes_up_space_cur(e)) { cur += cursor.offset }
    if (tree.funcs.takes_up_space_end(e)) { end += cursor.offset }

    for (let i = cursor.leaf_idx * LEAF_CHILDREN; i < idx_off; i++) {
      e = tree.leaf_values[i]
      cur += tree.funcs.content_len_cur(e)
      end += tree.funcs.content_len_end(e)
    }

    // Then recurse up.
    let p = tree.leaf_parents[cursor.leaf_idx]
    let last_child = cursor.leaf_idx

    while (p !== NULL_IDX) {
      let base = p * NODE_CHILDREN
      for (let i = 0; i < NODE_CHILDREN; i++) {
        let c = tree.node_child_indexes[base + i]
        if (c == last_child) break
        cur += tree.node_child_width[(base + i) * 2]
        end += tree.node_child_width[(base + i) * 2 + 1]
      }

      last_child = p
      p = tree.node_parents[p]
    }

    assertEq(cur, exp_cur)
    assertEq(end, exp_end)
  }
}

/** Returns 0 if they match, negative if a < b, positive if a > b. */
export function ct_cmp_cursor<V>(tree: ContentTreeInner<V>, a: ContentCursor, b: ContentCursor): number {
  if (a.leaf_idx === b.leaf_idx) {
    return (a.elem_idx == b.elem_idx)
      ? a.offset - b.offset
      : a.elem_idx - b.elem_idx
  } else {
    // Recursively walk up the trees to find a common ancestor. Because a b-tree is always
    // perfectly balanced, we can walk in lock step until both nodes are the same.
    let c1 = a.leaf_idx
    let n1 = tree.leaf_parents[c1]
    let c2 = b.leaf_idx
    let n2 = tree.leaf_parents[c2]

    while (n1 !== n2) {
      // Go up the tree.
      c1 = n1
      n1 = tree.node_parents[n1]
      c2 = n2
      n2 = tree.node_parents[n2]

      assertNe(n1, NULL_IDX)
      assertNe(n2, NULL_IDX)
    }

    // Find the relative order of c1 and c2.
    assertEq(n1, n2);
    assertNe(c1, c2);

    return node_idx_of_child(tree, n1, c1) - node_idx_of_child(tree, n1, c2)
  }
}


// *** Content tree methods! (finally!) ***

export function ctCreate<V>(funcs: ContentTreeFuncs<V>, notify: NotifyFn<V> = () => { }): ContentTreeInner<V> {
  DBG: {
    assertEq(funcs.content_len_cur(funcs.none()), 0)
    assertEq(funcs.content_len_end(funcs.none()), 0)
    assertEq(funcs.exists(funcs.none()), false)
  }

  let tree: ContentTreeInner<V> = {
    leaf_values: [],
    leaf_next: [],
    leaf_parents: [],

    node_child_indexes: [],
    node_child_width: [],
    node_parents: [],

    height: 0,
    root: 0, // pushLeaf() will return 0.

    _cur_len: 0,
    _end_len: 0,

    funcs,

    cursor: null,
    cursor_cur: MAX_BOUND,
    cursor_end: MAX_BOUND,

    upd_leaf: MAX_BOUND,
    upd_cur: 0,
    upd_end: 0,

    notify,
  }

  // We've said the root item is 0. pushLeaf will push a new root with index 0.
  pushLeaf(tree)
  return tree
}

export function ctClear<V>(tree: ContentTreeInner<V>) {
  tree.leaf_values.length = 0
  tree.leaf_next.length = 0
  tree.leaf_parents.length = 0

  tree.node_child_indexes.length = 0
  tree.node_child_width.length = 0
  tree.node_parents.length = 0

  tree.height = 0
  tree.root = 0
  tree._cur_len = 0
  tree._end_len = 0

  tree.cursor = null
  tree.cursor_cur = tree.cursor_end = MAX_BOUND

  tree.upd_leaf = MAX_BOUND
  tree.upd_cur = tree.upd_end = 0

  // I could probably just set leaf length to 1, then clear the content. This is
  // potentially less buggy at least.
  pushLeaf(tree)
}

export function ctSetSingleItem<V>(tree: ContentTreeInner<V>, item: V) {
  DBG: {
    assert(!tree.funcs.exists(tree.leaf_values[0]))
    assertEq(tree.upd_leaf, MAX_BOUND)
  }

  tree.notify(item, 0)
  tree._cur_len = tree.funcs.content_len_cur(item)
  tree._end_len = tree.funcs.content_len_end(item)
  tree.leaf_values[0] = item
}

export function ctIsEmpty(tree: ContentTreeInner<any>): boolean {
  assert(tree.leaf_values.length > 0) // Must always be true since we add a leaf right away.
  return !tree.funcs.exists(tree.leaf_values[0])
}

function create_new_root_node<V>(tree: ContentTreeInner<V>, child_a: number, child_b: number, b_cur: number, b_end: number): NodeIdx {
  // This is called when the old root item (a leaf or a node) becomes too big
  // and we need to create and assign a new root pointing to the top level items.

  tree.height++

  const new_root_idx = pushNode(tree)

  let b = new_root_idx * NODE_CHILDREN
  tree.node_child_indexes[b + 0] = child_a
  tree.node_child_indexes[b + 1] = child_b

  b = new_root_idx * NODE_CHILDREN * 2

  // new_root.child_width[0] = self.total_len - b_size;
  tree.node_child_width[b + 0] = tree._cur_len - b_cur
  tree.node_child_width[b + 1] = tree._end_len - b_end

  // new_root.child_width[1] = b_size;
  tree.node_child_width[b + 2] = b_cur
  tree.node_child_width[b + 3] = b_end

  tree.root = new_root_idx
  return new_root_idx
}

export function ct_total_cur_len(tree: ContentTreeInner<any>): number {
  return tree._cur_len + tree.upd_cur
}
export function ct_total_end_len(tree: ContentTreeInner<any>): number {
  return tree._end_len + tree.upd_end
}


// Split a full internal node into 2 nodes.
function split_node<V>(tree: ContentTreeInner<V>, old_idx: NodeIdx, children_are_leaves: boolean): NodeIdx {
  const old_node_base = old_idx * NODE_CHILDREN
  const old_node_width_base = old_idx * NODE_CHILDREN * 2

  // The old leaf must be full before we split it.
  DBG: {
    assert(node_is_full(tree, old_idx))
  }

  // Calculate the split size - sum of widths from SPLIT_POINT to end
  let split_cur = 0
  let split_end = 0
  for (let i = NODE_SPLIT_POINT; i < NODE_CHILDREN; i++) {
    split_cur += tree.node_child_width[old_node_width_base + i * 2 + 0]
    split_end += tree.node_child_width[old_node_width_base + i * 2 + 1]
  }

  const new_node_idx = pushNode(tree)

  const new_node_base = new_node_idx * NODE_CHILDREN
  const new_node_width_base = new_node_idx * NODE_CHILDREN * 2

  // Copy children from the old node's second half to the new node's first half
  tree.node_child_indexes.copyWithin(new_node_base, old_node_base + NODE_SPLIT_POINT, old_node_base + NODE_CHILDREN)
  tree.node_child_width.copyWithin(new_node_width_base, old_node_width_base + NODE_SPLIT_POINT * 2, old_node_width_base + NODE_CHILDREN * 2)
  tree.node_child_indexes.fill(NULL_IDX, old_node_base + NODE_SPLIT_POINT, old_node_base + NODE_CHILDREN)

  // Update the parent pointers for all children of the new node
  if (children_are_leaves) {
    for (let i = 0; i < NODE_SPLIT_POINT; i++) {
      const child_idx = tree.node_child_indexes[new_node_base + i]
      assert(child_idx < tree.leaf_parents.length)
      tree.leaf_parents[child_idx] = new_node_idx
    }
  } else {
    for (let i = 0; i < NODE_SPLIT_POINT; i++) {
      const child_idx = tree.node_child_indexes[new_node_base + i]
      assert(child_idx < tree.node_parents.length)
      tree.node_parents[child_idx] = new_node_idx
    }
  }

  // Now handle the parent.

  // It would be much nicer to do this above earlier - and in earlier versions I did.
  // The problem is that both create_new_root_node and insert_into_node can insert new items
  // into self.nodes. If that happens, the new node index we're expecting to use is used by
  // another node. Hence, we need to call self.nodes.push() before calling any other function
  // which modifies the node list.
  if (old_idx === tree.root) {
    // We'll make a new root
    let parent = create_new_root_node(tree, old_idx, new_node_idx, split_cur, split_end)
    tree.node_parents[old_idx] = parent
    tree.node_parents[new_node_idx] = parent
  } else {
    let parent = tree.node_parents[old_idx]
    tree.node_parents[new_node_idx] = split_child_of_node(tree, parent, old_idx, new_node_idx, split_cur, split_end, false)
  }

  return new_node_idx
}

function split_child_of_node<V>(
  tree: ContentTreeInner<V>,
  node_idx: NodeIdx,
  child_idx: number,
  new_child_idx: number,
  stolen_cur: number,
  stolen_end: number,
  children_are_leaves: boolean
): NodeIdx {

  // Find the position of the child in the node
  let child_pos = node_idx_of_child(tree, node_idx, child_idx)

  if (node_is_full(tree, node_idx)) {
    // This node is full, we need to split it first
    const new_node = split_node(tree, node_idx, children_are_leaves)

    if (child_pos >= NODE_SPLIT_POINT) {
      // We're inserting into the new node
      child_pos -= NODE_SPLIT_POINT
      node_idx = new_node
    }
  }

  // Update the width of the existing child
  let child_width_base = (node_idx * NODE_CHILDREN + child_pos) * 2
  tree.node_child_width[child_width_base + 0] -= stolen_cur
  tree.node_child_width[child_width_base + 1] -= stolen_end

  // Insert the new child after the existing one
  const insert_pos = child_pos + 1

  let node_base = node_idx * NODE_CHILDREN

  // Index.
  tree.node_child_indexes.copyWithin(
    node_base + insert_pos + 1,
    node_base + insert_pos,
    node_base + NODE_CHILDREN - 1
  )
  tree.node_child_indexes[node_base + insert_pos] = new_child_idx

  // Width
  let width_base = node_base * 2
  tree.node_child_width.copyWithin(
    width_base + (insert_pos + 1) * 2,
    width_base + insert_pos * 2,
    width_base + (NODE_CHILDREN - 1) * 2
  )
  tree.node_child_width[width_base + insert_pos * 2 + 0] = stolen_cur
  tree.node_child_width[width_base + insert_pos * 2 + 1] = stolen_end

  return node_idx
}

function split_leaf<V>(tree: ContentTreeInner<V>, old_idx: LeafIdx): LeafIdx {
  // This function splits a full leaf node in the middle, into 2 new nodes.
  // The result is two nodes - old_leaf with items 0..N/2 and new_leaf with items N/2..N.

  const old_height = tree.height

  const new_leaf_idx = pushLeaf(tree)

  // The old leaf must be full before we split it.
  DBG: {
    assert(!leaf_has_space(tree, old_idx, 2))
  }

  // Calculate the size of the elements we're moving to the new leaf
  let new_cur = 0
  let new_end = 0
  const old_leaf_base = old_idx * LEAF_CHILDREN

  for (let i = LEAF_SPLIT_POINT; i < LEAF_CHILDREN; i++) {
    const val = tree.leaf_values[old_leaf_base + i]
    if (tree.funcs.exists(val)) {
      tree.notify(val, new_leaf_idx)
      new_cur += tree.funcs.content_len_cur(val)
      new_end += tree.funcs.content_len_end(val)
    } else {
      break
    }
  }

  let parent: NodeIdx
  if (old_height === 0) {
    // Insert this leaf into a new root node. This has to be the first node.
    parent = create_new_root_node(tree, old_idx, new_leaf_idx, new_cur, new_end)
    assertEq(parent, 0)
  } else {
    parent = tree.leaf_parents[old_idx]
    // The parent may change by calling split_child_of_node - since the node we're inserting
    // into may split off.
    parent = split_child_of_node(tree, parent, old_idx, new_leaf_idx, new_cur, new_end, true)
  }
  tree.leaf_parents[old_idx] = parent

  // Create the new leaf
  // const new_leaf_idx_actual = pushLeaf(tree)
  // assert(new_leaf_idx === new_leaf_idx_actual)
  const new_leaf_base = new_leaf_idx * LEAF_CHILDREN

  // Set up the new leaf's properties
  tree.leaf_next[new_leaf_idx] = tree.leaf_next[old_idx]
  tree.leaf_parents[new_leaf_idx] = parent

  // We'll steal the second half of the items in old_leaf.
  // Copy elements from old leaf to new leaf
  tree.leaf_values.copyWithin(new_leaf_base, old_leaf_base + LEAF_SPLIT_POINT, old_leaf_base + LEAF_CHILDREN)
  // for (let i = 0; i < LEAF_SPLIT_POINT; i++) {
  //   tree.leaf_values[new_leaf_base + i] = tree.leaf_values[old_leaf_base + LEAF_SPLIT_POINT + i]
  // }

  // Clear the second half of the old leaf
  for (let i = LEAF_SPLIT_POINT; i < LEAF_CHILDREN; i++) {
    tree.leaf_values[old_leaf_base + i] = tree.funcs.none() // TODO: Reuse a "none object".
  }

  // Update the old leaf's next pointer
  tree.leaf_next[old_idx] = new_leaf_idx

  return new_leaf_idx
}

function make_space_in_leaf_for<V>(
  tree: ContentTreeInner<V>,
  space_wanted: number,
  leaf_idx: LeafIdx,
  elem_idx: number
): [LeafIdx, number] {
  assert(space_wanted === 1 || space_wanted === 2)

  if (leaf_has_space(tree, leaf_idx, space_wanted)) {
    // There's enough space in the current leaf, so shift elements to make room
    const leaf_base = leaf_idx * LEAF_CHILDREN
    tree.leaf_values.copyWithin(
      leaf_base + elem_idx + space_wanted,
      leaf_base + elem_idx,
      leaf_base + LEAF_CHILDREN - space_wanted
    )
    // for (let i = LEAF_CHILDREN - space_wanted - 1; i >= elem_idx; i--) {
    //   tree.leaf_values[leaf_base + i + space_wanted] = tree.leaf_values[leaf_base + i]
    // }
  } else {
    // Not enough space, need to split the leaf
    if (tree.upd_leaf === leaf_idx) flush_delta(tree) // Easier than updating cursor correctly.

    const new_node = split_leaf(tree, leaf_idx)

    if (elem_idx >= LEAF_SPLIT_POINT) {
      // We're inserting into the newly created node
      leaf_idx = new_node
      elem_idx -= LEAF_SPLIT_POINT
    }

    // Now shift elements in the target leaf
    const leaf_base = leaf_idx * LEAF_CHILDREN
    tree.leaf_values.copyWithin(
      leaf_base + elem_idx + space_wanted,
      leaf_base + elem_idx,
      leaf_base + LEAF_SPLIT_POINT // We know there are exactly LEAF_SPLIT_POINT items in the leaf.
    )

    // for (let i = (leaf_idx === new_node ? LEAF_SPLIT_POINT : LEAF_CHILDREN) - space_wanted - 1; i >= elem_idx; i--) {
    //   tree.leaf_values[new_leaf_base + i + space_wanted] = tree.leaf_values[new_leaf_base + i]
    // }

  }
  return [leaf_idx, elem_idx]
}

// Splice in an item, and optionally remainder afterwards. Returns the (leaf_idx, elem_idx) of
// the inserted item, but NOT the remainder.
function splice_in_internal<V>(
  tree: ContentTreeInner<V>,
  item: V,
  remainder: V | null,
  leaf_idx: LeafIdx,
  elem_idx: number,
  notify_here: boolean
): [LeafIdx, number] {
  const space_needed = 1 + (remainder !== null ? 1 : 0)
  const [new_leaf_idx, new_elem_idx] = make_space_in_leaf_for(tree, space_needed, leaf_idx, elem_idx)

  // Only call notify if we're either notifying in all cases, or if the item is inserted
  // into a different leaf than we were passed.
  const moved = new_leaf_idx !== leaf_idx
  if (notify_here || moved) {
    leaf_idx = new_leaf_idx
    tree.notify(item, leaf_idx)
  }

  inc_delta_update_by(tree, leaf_idx, item)
  // delta.upd_cur += tree.funcs.content_len_cur(item)
  // delta.upd_end += tree.funcs.content_len_end(item)

  const new_leaf_base = leaf_idx * LEAF_CHILDREN
  tree.leaf_values[new_leaf_base + new_elem_idx] = item

  if (remainder !== null) {
    if (moved) tree.notify(remainder, leaf_idx)
    inc_delta_update_by(tree, leaf_idx, remainder)
    tree.leaf_values[new_leaf_base + new_elem_idx + 1] = remainder
  }

  return [leaf_idx, new_elem_idx]
}

export function ct_insert<V>(
  tree: ContentTreeInner<V>,
  item: V,
  cursor: ContentCursor,
  notify_here: boolean
): void {
  DBG: {
    assert(tree.funcs.exists(item))
  }

  let leaf_idx = cursor.leaf_idx
  let elem_idx = cursor.elem_idx
  let offset = cursor.offset
  const leaf_base = leaf_idx * LEAF_CHILDREN

  let remainder: V | null = null
  if (offset === 0 && elem_idx > 0) {
    // Roll the cursor back to opportunistically see if we can append
    elem_idx -= 1
    offset = tree.funcs.raw_len(tree.leaf_values[leaf_base + elem_idx])
    // No remainder needed in this case
  } else if (offset === 0 || offset === tree.funcs.raw_len(tree.leaf_values[leaf_base + elem_idx])) {
    // No need for remainder in this case either
  } else {
    // We're in the middle of an item. Split it
    const entry = tree.leaf_values[leaf_base + elem_idx]
    remainder = tree.funcs.truncate(entry, offset)
    dec_delta_update_by(tree, leaf_idx, remainder)
    // We don't need to update cursor since it's already where it needs to be
  }

  if (offset !== 0) {
    DBG: {
      assertEq(offset, tree.funcs.raw_len(tree.leaf_values[leaf_base + elem_idx]))
    }

    // We're at the end of an element. Try and append here.
    const cur_entry = tree.leaf_values[leaf_base + elem_idx]
    if (tree.funcs.tryAppend(cur_entry, item)) {
      // In this case, item should not have been modified. Which is important - since we need to
      // read back some of its properties here!
      inc_delta_update_by(tree, leaf_idx, item)
      if (notify_here) {
        tree.notify(item, leaf_idx)
      }

      cursor.elem_idx = elem_idx
      cursor.offset = tree.funcs.raw_len(cur_entry)

      if (remainder !== null) {
        const [leaf_idx_2, elem_idx_2] = splice_in_internal(
          tree, remainder, null, leaf_idx, elem_idx + 1, notify_here
        )

        // If the remainder was inserted into a new item, we might need to update the
        // cursor.
        if (leaf_idx_2 !== leaf_idx) {
          if (elem_idx_2 > 0) {
            // This is a bit of a hack. Move the cursor to the item before the remainder.
            cursor.leaf_idx = leaf_idx_2
            cursor.elem_idx = elem_idx_2 - 1
          // } else {
          //   // The remainder is on a subsequent element. This is fine, but now delta
          //   // refers to the item the remainder is on, not the cursor element.
          //   // So we need to flush it.
          //   // TODO: Urgh this is gross. Rewrite me!
          //   flush_delta_and_clear(tree, leaf_idx_2, cursor)
          }
        }
      }
      return
    }

    // Insert in the next slot
    elem_idx += 1 // NOTE: Cursor might point past the end of the node.

    // Try and prepend to the start of the next item
    const next_entry_idx = leaf_base + elem_idx
    if (remainder === null
      && elem_idx < LEAF_CHILDREN
      && tree.funcs.exists(tree.leaf_values[next_entry_idx]))
    {
      const cur_entry = tree.leaf_values[next_entry_idx]

      // Try to prepend the item to the existing item.
      let len = tree.funcs.raw_len(item)
      if (tree.funcs.tryAppend(item, cur_entry)) {
        // inc_delta_update_by(tree, leaf_idx, item)
        // I can't just use inc_delta_update_by(item) because it now includes cur_entry.
        set_delta_to(tree, leaf_idx)
        if (tree.funcs.takes_up_space_cur(item)) tree.upd_cur += len
        if (tree.funcs.takes_up_space_end(item)) tree.upd_end += len

        if (notify_here) {
          tree.notify(item, leaf_idx)
        }

        // Replace the item with (the newly prepended) cur_entry.
        tree.leaf_values[next_entry_idx] = item
        cursor.elem_idx = elem_idx
        cursor.offset = len
        return
      }
    }
  }

  cursor.offset = tree.funcs.raw_len(item)
  const [new_leaf_idx, new_elem_idx] = splice_in_internal(
    tree, item, remainder, leaf_idx, elem_idx, notify_here
  )
  cursor.leaf_idx = new_leaf_idx
  cursor.elem_idx = new_elem_idx
}

/**
 * Mutate the next item under the cursor, with maximum size of replace_max.
 * Map function is expected to mutate the item in-place.
 *
 * Mutation function must not modify the item's length.
 */
export function ct_mutate_entry<V, R>(
  tree: ContentTreeInner<V>,
  cursor: ContentCursor,
  replace_max: number,
  map_fn: (v: V) => R
): [number, R] {
  if (!cursor_roll_next_item(tree, cursor)) {
    throw new Error("Cannot mutate at end of data structure");
  }

  const leaf = cursor.leaf_idx
  const leaf_base = leaf * LEAF_CHILDREN
  const entry = tree.leaf_values[leaf_base + cursor.elem_idx]
  const entry_len = tree.funcs.raw_len(entry)

  if (cursor.offset === 0 && entry_len <= replace_max) {
    // Replace in-place
    dec_delta_update_by(tree, leaf, entry)
    const r = map_fn(entry)
    inc_delta_update_by(tree, leaf, entry)
    cursor.offset = entry_len

    // We'll also do a brief best-effort attempt at merging this modified item with
    // subsequent items in the leaf
    let modified_entry = tree.leaf_values[leaf_base + cursor.elem_idx]
    const scan_start = cursor.elem_idx + 1
    let elem_idx2 = scan_start

    while (elem_idx2 < LEAF_CHILDREN) {
      const next_entry = tree.leaf_values[leaf_base + elem_idx2]
      if (!tree.funcs.exists(next_entry)
        || !tree.funcs.tryAppend(modified_entry, next_entry)) break

      elem_idx2++
    }

    if (elem_idx2 > scan_start) {
      tree.leaf_values[leaf_base + cursor.elem_idx] = modified_entry
      // Clear the now-merged items.
      tree.leaf_values
        .copyWithin(
          leaf_base + scan_start,
          leaf_base + elem_idx2,
          leaf_base + LEAF_CHILDREN
        )
        .fill(tree.funcs.none(),
          leaf_base + LEAF_CHILDREN - (elem_idx2 - scan_start),
          leaf_base + LEAF_CHILDREN
        )
    }

    return [entry_len, r]
  }

  // Otherwise we've got ourselves a situation.
  if (cursor.offset > 0) {
    let rest = tree.funcs.truncate(entry, cursor.offset)
    dec_delta_update_by(tree, leaf, rest)

    const rest_len = tree.funcs.raw_len(rest)
    if (rest_len <= replace_max) {
      // If the remaining piece is small enough, simply apply the mutation.
      const r = map_fn(rest)
      // Reinsert (splice) the mutated part back into the tree.
      ct_insert(tree, rest, cursor, false)
      return [rest_len, r]
    } else {
      // Ugh. We're modifying the middle of this item. We'll use splice_in_internal, which
      // does not try and join the updated item - since its more convenient, and we
      // probably can't join it to nearby items anyway.
      const remainder = tree.funcs.truncate(rest, replace_max)
      const r = map_fn(rest)
      cursor.offset = replace_max // Cursor now points just after the modified part.
      // Splice in the mutated portion and the remainder.
      const [new_leaf_idx, new_elem_idx] = splice_in_internal(
        tree,
        rest,
        remainder,
        cursor.leaf_idx,
        cursor.elem_idx + 1,
        false
      )
      cursor.leaf_idx = new_leaf_idx
      cursor.elem_idx = new_elem_idx
      return [replace_max, r]
    }
  } else {
    assert(entry_len > replace_max)
    // In this case, we need to cut the existing item down and modify the start of it.
    // There's a few ways to do this. The simplest is to just chop out the modified bit and
    // re-insert it.
    let e = tree.funcs.truncate_keeping_right(entry, replace_max)
    dec_delta_update_by(tree, leaf, e)
    // The cursor offset is already at 0.
    const r = map_fn(e)
    // Reinsert using ctInsert (which will try to merge with the previous element if possible).
    ct_insert(tree, e, cursor, false)
    // And the cursor will be moved to right after the item in all cases.
    return [replace_max, r]
  }
}



// *** Cursors ***


export function ct_cursor_at_start<V>(tree: ContentTreeInner<V>): ContentCursor {
  if (tree.cursor) {
    // Wipe out any emplaced cursor. There can be only one!
    // flush_delta_and_clear(tree, tree.cursor.leaf_idx, tree.cursor)
    tree.cursor = null
    tree.cursor_cur = tree.cursor_end = MAX_BOUND
  }

  // Could reuse the cursor object here, but I don't think this function is hot.
  return cursor_at_start_nothing_emplaced(tree)
  // return {
  //   leaf_idx: 0,
  //   elem_idx: 0,
  //   offset: 0,
  //   // upd_cur: 0,
  //   // upd_end: 0
  // }
}

function cursor_at_start_nothing_emplaced<V>(tree: ContentTreeInner<V>): ContentCursor {
  assertEq(tree.cursor, null)
  return {
    leaf_idx: 0,
    elem_idx: 0,
    offset: 0,
    // upd_cur: 0,
    // upd_end: 0
  }
}

/// This function blindly assumes the item is definitely in the recursive children.
///
/// Returns (child index, relative end pos of the index, len remaining).
function find_cur_pos_in_node(tree: ContentTreeInner<any>, idx: NodeIdx, at_cur_pos: number): [number, number, number] {
  let end_pos_offset = 0

  let base = idx * NODE_CHILDREN
  let width_base = idx * NODE_CHILDREN * 2
  for (let i = 0; i < NODE_CHILDREN; i++) {
    // let width = node.child_width[i];
    let width_cur = tree.node_child_width[width_base + i * 2 + 0]
    let width_end = tree.node_child_width[width_base + i * 2 + 1]

    if (at_cur_pos < width_cur) {
      return [tree.node_child_indexes[base + i], end_pos_offset, at_cur_pos]
    }

    at_cur_pos -= width_cur
    end_pos_offset += width_end
  }

  throw Error("Position not in node")
}

/// Returns (index, end_pos, offset).
function find_cur_pos_in_leaf(tree: ContentTreeInner<any>, idx: LeafIdx, at_cur_pos: number): [number, number, number] {
  let end_pos = 0;

  let base = idx * NODE_CHILDREN
  let width_base = idx * NODE_CHILDREN * 2

  for (let i = 0; i < LEAF_CHILDREN; i++) {
    let val = tree.leaf_values[base + i]
    let cur = tree.funcs.content_len_cur(val)
    let end = tree.funcs.content_len_end(val)

    if (at_cur_pos < cur) {
      // We return the end pos of the offset position, not just the start of this child.
      end_pos += tree.funcs.takes_up_space_end(val) ? at_cur_pos : 0
      return [i, end_pos, at_cur_pos]
    }
    at_cur_pos -= cur
    end_pos += end
  }

  throw Error("Position not in leaf")
}

// Returns the end length slid past
function slide_cursor_to_next_content<V>(tree: ContentTreeInner<V>, cursor: ContentCursor): number {
  let leaf_base = cursor.leaf_idx * LEAF_CHILDREN
  const e = tree.leaf_values[leaf_base + cursor.elem_idx];

  if (!tree.funcs.exists(e)) return 0

  let len = tree.funcs.raw_len(e)
  if (tree.funcs.takes_up_space_cur(e) && cursor.offset < len) {
    return 0;
  }

  let end_slide_len = tree.funcs.takes_up_space_end(e)
    ? len - cursor.offset
    : 0

  cursor.elem_idx++
  cursor.offset = 0

  // let leaf = cursor.leaf_idx

  while (true) {
    // This walks linearly through the nodes. It would be "big-O faster" to walk up and down
    // the tree in this case, but I think this will usually be faster in practice.
    if (cursor.elem_idx >= LEAF_CHILDREN ||
        !tree.funcs.exists(tree.leaf_values[leaf_base + cursor.elem_idx])) {
      // Go to next leaf
      const next_leaf = tree.leaf_next[cursor.leaf_idx]

      if (next_leaf === NULL_IDX) throw new Error("Unreachable: cursor past end of list")

      // flush_delta_and_clear(tree, cursor.leaf_idx, cursor)
      cursor.leaf_idx = next_leaf
      leaf_base = cursor.leaf_idx * LEAF_CHILDREN
      cursor.elem_idx = 0
    }

    const e = tree.leaf_values[leaf_base + cursor.elem_idx]
    if (tree.funcs.takes_up_space_cur(e)) {
      break
    }

    end_slide_len += tree.funcs.content_len_end(e)
    cursor.elem_idx++
  }

  return end_slide_len
}

export function ct_cursor_before_cur_pos<V>(tree: ContentTreeInner<V>, content_pos: number): [number, ContentCursor] {
  let cursor = tree.cursor

  if (cursor) {
    tree.cursor = null
    let cur_cur = tree.cursor_cur
    let cur_end = tree.cursor_end
    tree.cursor_cur = tree.cursor_end = MAX_BOUND

    if (cur_cur === content_pos) {
      // We can reuse the existing cursor
      let end = cur_end + slide_cursor_to_next_content(tree, cursor)
      return [end, cursor]
    }

    // flush_delta(tree, cursor.leaf_idx, cursor)
  }

  // Otherwise flush the delta. We need all the positions correct to scan.
  flush_delta(tree)

  // Make a cursor by descending from the root
  let idx = tree.root
  let end_pos = 0
  let content_pos_remaining = content_pos

  // Descend through the tree to find the right position
  for (let _h = 0; _h < tree.height; _h++) {
    // TODO: This function is only used from this one place. Given it returns a tuple,
    // consider inlining it to avoid the allocation.
    let [child_idx, rel_end_pos, cpr] = find_cur_pos_in_node(tree, idx, content_pos_remaining)
    end_pos += rel_end_pos
    content_pos_remaining = cpr
    idx = child_idx
  }

  // Now find the position within the leaf
  const leaf_base = idx * LEAF_CHILDREN
  let [elem_idx, rel_end_pos, offset] = find_cur_pos_in_leaf(tree, idx, content_pos_remaining)

  // We're guaranteed that the item under elem_idx has size in CUR. Well, unless the tree is empty.
  DBG: {
    assert((content_pos == 0 && ctIsEmpty(tree))
      || tree.funcs.takes_up_space_cur(tree.leaf_values[leaf_base + elem_idx]))
  }

  // Create and return the cursor
  cursor = {
    leaf_idx: idx,
    elem_idx,
    offset,
    // upd_cur: 0,
    // upd_end: 0
  }

  return [
    end_pos + rel_end_pos,
    cursor
  ]
}

export function ct_emplace_cursor<V>(tree: ContentTreeInner<V>, cur_pos: number, end_pos: number, cursor: ContentCursor) {
  assertEq(tree.cursor, null)

  tree.cursor = cursor
  tree.cursor_cur = cur_pos
  tree.cursor_end = end_pos

  DBG: {
    // In debug mode, verify that the cursor is pointing to the correct position
    assert_cursor_at(tree, cursor, cur_pos, end_pos)
  }
}

// This function is useful because we can fetch a cursor without a delta, and the delta won't get
// flushed until later.
export function ct_emplace_cursor_unknown<V>(tree: ContentTreeInner<V>, cursor: ContentCursor) {
  assertEq(tree.cursor, null)

  tree.cursor = cursor;
  tree.cursor_cur = tree.cursor_end = MAX_BOUND;
}

/** Try and find the named LV within the current leaf position */
export function ct_try_find_item_at_cursor<V>(tree: ContentTreeInner<V>, lv: LV): ContentCursor | null {
  let cursor = tree.cursor
  if (cursor) {
    tree.cursor = null

    let base = cursor.leaf_idx * LEAF_CHILDREN
    for (let i = 0; i < LEAF_CHILDREN; i++) {
      let e = tree.leaf_values[base + i]
      if (!tree.funcs.exists(e)) break

      let offset = tree.funcs.find(e, lv)
      if (offset >= 0) {
        // Yay we found it!
        cursor.elem_idx = i
        cursor.offset = offset
        return cursor
      }
    }

    // In rust we just flush the cursor.
    tree.cursor = cursor
  }

  return null
}

// Get a cursor to an item within the given leaf. The cursor returned MUST NOT
// be used to mutate the content tree. (This makes a lot more sense in rust - it
// might be worth revisiting this!)
export function ct_cursor_before_item<V>(tree: ContentTreeInner<V>, id: LV, leaf_idx: LeafIdx): ContentCursor {
  const leaf_base = leaf_idx * LEAF_CHILDREN

  for (let elem_idx = 0; elem_idx < LEAF_CHILDREN; elem_idx++) {
    const e = tree.leaf_values[leaf_base + elem_idx]
    if (!tree.funcs.exists(e)) break

    const offset = tree.funcs.find(e, id)
    if (offset >= 0) {
      return {
        leaf_idx,
        elem_idx,
        offset,
      }
    }
  }

  throw new Error("Could not find element in leaf")
}

/**
 * This function is the same as ct_cursor_before_item but it returns a reusable
 * cursor from the tree.
 */
export function ct_cached_cursor_before_item<V>(
  tree: ContentTreeInner<V>,
  id: LV,
  leaf_idx: LeafIdx
): ContentCursor {
  let cursor = tree.cursor
  if (cursor) {
    tree.cursor = null;

    // const cur = tree.cursor_cur;
    // const end = tree.cursor_end;
    tree.cursor_cur = tree.cursor_end = MAX_BOUND;

    const item_offset = cur_value_offset(cursor);
    const e = tree.leaf_values[item_offset];

    const actual_offset = tree.funcs.find(e, id);
    if (actual_offset >= 0) {
      cursor.offset = actual_offset;
      return cursor
    } else if (cursor.elem_idx > 0) {
      // Search within the previous item.
      const prev_elem = tree.leaf_values[item_offset - 1];
      const actual_offset = tree.funcs.find(prev_elem, id);

      if (actual_offset >= 0) {
        cursor.elem_idx -= 1;
        cursor.offset = actual_offset;

        return cursor
      }
    }
  }

  return ct_cursor_before_item(tree, id, leaf_idx)
}

function first_leaf<V>(tree: ContentTreeInner<V>): LeafIdx {
  DEV: {
    let idx = tree.root
    for (let i = 0; i < tree.height; i++) {
      idx = tree.node_child_indexes[idx * NODE_CHILDREN]
    }
    assert(idx === 0, "First leaf should be at index 0")
  }
  return 0
}

export function ct_print_tree(tree: ContentTreeInner<any>) {
  console.log('------ TREE ------')
  console.log('cur size', ct_total_cur_len(tree), '/ internal cached', tree._cur_len)
  console.log('end size', ct_total_end_len(tree), '/ internal cached', tree._end_len)
  console.log('cursor', tree.cursor)
  if (tree.cursor) console.log('cursor at cur', tree.cursor_cur, 'end', tree.cursor_end)

  console.log('height', tree.height)
  console.log('root', tree.root)

  // Indent string
  const indStr = (depth: number) => '  '.repeat(depth)

  const printNode = (indent: number, depth: number, idx: number) => {
    if (depth === tree.height) {
      // Leaf node
      // console.log(`${indent(depth)}Leaf ${idx} parent ${tree.leaf_parents[idx]}`)

      if (tree.upd_leaf === idx) {
        console.log(`${indStr(indent+1)}UPDATE IS HERE! upd_cur: ${tree.upd_cur} / upd_end: ${tree.upd_end}`)
      }

      const base = idx * LEAF_CHILDREN
      for (let i = 0; i < LEAF_CHILDREN; i++) {
        let val = tree.leaf_values[base + i]
        if (!tree.funcs.exists(val)) break
        console.log(`${indStr(indent+1)}${i}: ${JSON.stringify(val)}`)
      }
      console.log(`${indStr(indent+1)}next leaf ${tree.leaf_next[idx]}`)
    } else {
      // Internal node
      console.log(`${indStr(indent)}Node ${idx} parent ${tree.node_parents[idx]}`)
      const base = idx * NODE_CHILDREN
      for (let i = 0; i < NODE_CHILDREN; i++) {
        let child = tree.node_child_indexes[base + i]
        if (child >= 0) {
          const cur = tree.node_child_width[(base + i)*2 + 0]
          const end = tree.node_child_width[(base + i)*2 + 1]
          console.log(`${indStr(indent+1)}${i}: Node Child ${child} width cur ${cur} / end ${end}`)

          const stored_parent = depth + 1 === tree.height
            ? tree.leaf_parents[child]
            : tree.node_parents[child]
          if (stored_parent !== idx) {
            console.error(`${indStr(indent+1)}Child has invalid stored parent: Stored ${stored_parent} != ${idx}`)
          }

          printNode(indent + 1, depth + 1, child)
        }
      }
    }
  }

  printNode(0, 0, tree.root)
  console.log('----------------')
}

// On the walk this returns the size of all children (recursive) and the expected next visited
// leaf idx.
//
// Returns [subtree_size, expected next leaf, pending delta (if any)]
function debug_check_walk_internal<V>(
  tree: ContentTreeInner<V>,
  idx: number,
  height: number,
  expect_next_leaf_idx: LeafIdx,
  expect_parent: NodeIdx
): [Delta, LeafIdx, Delta | null] {
  if (height === tree.height) {
    // This is a leaf node
    assert(idx < tree.leaf_next.length)
    assertEq(tree.leaf_parents[idx], expect_parent)
    assertEq(idx, expect_next_leaf_idx)

    let leaf_size = { cur: 0, end: 0 }
    for (let i = 0; i < LEAF_CHILDREN; i++) {
      const e = tree.leaf_values[idx * LEAF_CHILDREN + i]
      if (tree.funcs.exists(e)) {
        leaf_size.cur += tree.funcs.content_len_cur(e)
        leaf_size.end += tree.funcs.content_len_end(e)
      }
    }

    let delta = tree.upd_leaf === idx
      ? { cur: tree.upd_cur, end: tree.upd_end }
      : null

    return [leaf_size, tree.leaf_next[idx], delta]
  } else {
    // This is an internal node
    assert(idx < tree.node_parents.length)
    assert(tree.node_parents[idx] === expect_parent)

    let actual_node_size = { cur: 0, end: 0 }
    let delta = null

    for (let i = 0; i < NODE_CHILDREN; i++) {
      const child_idx = tree.node_child_indexes[idx * NODE_CHILDREN + i]
      if (child_idx === NULL_IDX) {
        assert(i >= 1, "All nodes must have at least 1 child")
        // All subsequent child_indexes must be NULL_IDX
        for (let j = i; j < NODE_CHILDREN; j++) {
          assertEq(tree.node_child_indexes[idx * NODE_CHILDREN + j], NULL_IDX)
        }
        break
      }

      const [actual_child_size, next_idx, d] =
        debug_check_walk_internal(tree, child_idx, height + 1, expect_next_leaf_idx, idx)

      expect_next_leaf_idx = next_idx

      if (d) {
        assert(!delta, "Delta should only appear once")
        delta = d
      }

      const width_base = (idx * NODE_CHILDREN + i) * 2
      let expect_cur = tree.node_child_width[width_base + 0]
      let expect_end = tree.node_child_width[width_base + 1]

      if (d) {
        expect_cur += d.cur
        expect_end += d.end
      }

      try {
        assertEq(actual_child_size.cur, expect_cur, 'current size does not match')
        assertEq(actual_child_size.end, expect_end, 'end size does not match')
      } catch (e) {
        console.error('When visiting node', idx, 'child', i, 'at idx', child_idx)
        throw e
      }

      actual_node_size.cur += expect_cur
      actual_node_size.end += expect_end
    }

    return [actual_node_size, expect_next_leaf_idx, delta]
  }
}

function debug_check_walk<V>(tree: ContentTreeInner<V>) {
  const [actual_len, last_next_ptr, delta] =
    debug_check_walk_internal(tree, tree.root, 0, 0, NULL_IDX);

  assertEq(actual_len.cur, ct_total_cur_len(tree))
  assertEq(actual_len.end, ct_total_end_len(tree))
  assertEq(last_next_ptr, NULL_IDX);

  if (tree.cursor != null) {
    // Make sure the cursor points to a valid value with a length at least offset.
    // (This is a regression if so)
    let elem = tree.leaf_values[cur_value_offset(tree.cursor)]
    assert(tree.funcs.raw_len(elem) >= tree.cursor.offset)
  }
}


export function ct_debug_check<V>(tree: ContentTreeInner<V>): void {
  // Invariants:
  // - Except for the root item, all leaves must have at least 1 data entry.
  // - The next pointers iterate through all items in sequence
  // - There is at least 1 leaf node
  // - The width of all items is correct.

  // This code does 2 traversals of the data structure:
  // 1. We walk the leaves by following next_leaf pointers in each leaf node
  // 2. We recursively walk the tree

  // Walk the tree structure
  debug_check_walk(tree);

  // Walk the leaves in sequence
  let leaves_visited = 0;
  let leaf_idx = first_leaf(tree);

  while (true) {
    leaves_visited++;

    if (leaf_idx === 0) {
      // First leaf can be empty - but only if the whole data structure is empty
      if (!tree.funcs.exists(tree.leaf_values[0])) {
        assert(tree.leaf_next[leaf_idx] === NULL_IDX);
        assert(tree._cur_len === 0);
        assert(tree._end_len === 0);
      }
    } else {
      // Only the first leaf can be empty
      assert(tree.funcs.exists(tree.leaf_values[leaf_idx * LEAF_CHILDREN]));
    }

    if (tree.leaf_next[leaf_idx] === NULL_IDX) break;
    leaf_idx = tree.leaf_next[leaf_idx];
  }

  assert(leaves_visited === tree.leaf_next.length);

  if (tree.height === 0) {
    assert(tree.root < tree.leaf_next.length);
  } else {
    assert(tree.root < tree.node_parents.length);
  }
}

// *** Iterators! ***

/**
 * Creates an iterator that yields all content values in the tree in order.
 */
export function* ct_iter<V>(tree: ContentTreeInner<V>): Generator<V> {
  if (ctIsEmpty(tree)) return;

  for (let leaf_idx = 0; leaf_idx !== NULL_IDX; leaf_idx = tree.leaf_next[leaf_idx]) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;

    // Iterate through elements in the current leaf
    for (let elem_idx = 0; elem_idx < LEAF_CHILDREN; elem_idx++) {
      const item = tree.leaf_values[leaf_base + elem_idx];
      if (!tree.funcs.exists(item)) break;

      yield item;
    }
  }
}

export function* ct_iter_rle<V>(tree: ContentTreeInner<V>): Generator<V> {
  // Might be cleaner to implement this using ct_iter above.
  if (ctIsEmpty(tree)) return;

  let last: V | null = null

  for (let leaf_idx = 0; leaf_idx !== NULL_IDX; leaf_idx = tree.leaf_next[leaf_idx]) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;

    for (let elem_idx = 0; elem_idx < LEAF_CHILDREN; elem_idx++) {
      const item = tree.leaf_values[leaf_base + elem_idx];
      if (!tree.funcs.exists(item)) break;

      if (last == null) last = {... item}
      else if (tree.funcs.tryAppend(last, item)) { }
      else {
        yield last
        last = {...item}
      }
    }
  }

  if (last != null) yield last
}

export function* ct_iter_leaves<V>(tree: ContentTreeInner<V>) {

  for (let leaf_idx = 0; leaf_idx !== NULL_IDX; leaf_idx = tree.leaf_next[leaf_idx]) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;

    yield {
      values: tree.leaf_values.slice(leaf_base, leaf_base + LEAF_CHILDREN),
      next: tree.leaf_next[leaf_idx],
      parent: tree.leaf_parents[leaf_idx],
    }
  }
}

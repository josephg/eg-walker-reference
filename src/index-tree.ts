// This code contains a b-tree implementation ported from index-tree in the diamond types
// repository.
//
// This is a run-length encodedm in-memory b-tree mapping from integer ranges to values.

import { assert, assertEq, assertNe } from './utils.js'

// TODO: Increase these numbers in production mode.
const NODE_CHILDREN = 4
const LEAF_CHILDREN = 4

const NODE_SPLIT_POINT = NODE_CHILDREN / 2
const LEAF_SPLIT_POINT = LEAF_CHILDREN / 2

// Type aliases just to make it a bit clearer what all the numbers represent.
type LV = number
type LeafIdx = number
type NodeIdx = number

/**
 * When we have a next pointer / parent pointer which does not exist, we use this sentinal value
 */
const NULL_IDX = -1
export const MAX_BOUND = Number.MAX_SAFE_INTEGER

interface IndexTree<V> {
  leaves: Leaves<V>,
  nodes: Nodes,

  height: number,
  root: number, // Leaf if height == 0, otherwise a node.

  cursor: IndexCursor,
  cursor_key: LV,

  // The original implementation contains a free node pool, but because nodes are basically never
  // actually removed in practice, and because the index tree as a whole is created then deleted
  // in one go, we'll just temporarily leak nodes instead.

  content_funcs: ITContent<V>,

  [Symbol.iterator](): Iterator<RleDRun<V>>
}

export interface ITContent<V> {
  /**
   * Try to append other to self. If possible, self is modified (if necessary) and true is
   * returned.
   */
  try_append(val: V, offset: number, other: V, other_len: number): boolean

  at_offset(val: V, offset: number): V

  eq(val: V, other: V, upto_len: number): boolean

  default(): V
}


/**
 * A leaf contains a fixed array of LEAF_CHILDREN bounds & values, a next_leaf pointer (integer)
 * and a parent pointer.
 *
 * To reduce the number of objects the GC needs to worry about, all leaves are packed into a
 * set of lists. This could be reduced further using typed arrays - or even, a single typed array
 * since everything we store is an int.
 */
interface Leaves<V> {
  // In these two arrays, each block of LEAF_CHILDREN items corresponds to a single leaf.
  bounds: LV[],
  values: V[],

  // And in these, each leaf contains exactly 1 value.
  next_leaves: number[],
  parents: NodeIdx[],
}

interface Nodes {
  // NODE_CHILDREN per node.
  keys: LV[],

  /// Child entries point to either another node or a leaf. We disambiguate using the height.
  /// The named LV is the first LV of the child data.
  ///
  /// Children are (usize::MAX, usize::MAX) if they are unset.
  child_indexes: number[],

  // 1 per node.
  parents: NodeIdx[],
}

interface IndexCursor {
  // The item pointed to by the cursor should still be in the CPU's L1 cache. I could cache some
  // properties of the cursor's leaf item here, but I think it wouldn't improve performance -
  // since we wouldn't be saving any memory loads anyway.
  leaf_idx: number,
  elem_idx: number,
}


/** Add a new leaf to the leaves object. The new leaf index is returned. */
function pushLeaf<V>(leaves: Leaves<V>): number {
  const newIdx = leaves.parents.length
  const newLength = newIdx + 1
  leaves.bounds.length = LEAF_CHILDREN * newLength
  leaves.bounds.fill(MAX_BOUND, newIdx * LEAF_CHILDREN, newLength * LEAF_CHILDREN)

  leaves.values.length = LEAF_CHILDREN * newLength

  leaves.parents.push(NULL_IDX)
  leaves.next_leaves.push(NULL_IDX)

  return newIdx
}

function pushRootLeaf<V>(leaves: Leaves<V>, funcs: ITContent<V>) {
  assertEq(leaves.parents.length, 0)

  // The initial tree implicitly contains a single default element which covers the entire range.
  pushLeaf(leaves)
  leaves.bounds[0] = 0
  leaves.values[0] = funcs.default()
}

function pushNode(nodes: Nodes): number {
  const newIdx = nodes.parents.length
  const newChildLength = (newIdx + 1) * NODE_CHILDREN

  nodes.keys.length = newChildLength
  nodes.keys.fill(MAX_BOUND, newIdx * NODE_CHILDREN, newChildLength)

  nodes.child_indexes.length = newChildLength
  nodes.child_indexes.fill(NULL_IDX, newIdx * NODE_CHILDREN, newChildLength)

  nodes.parents.push(NULL_IDX)
  return newIdx
}


export function it_create<V>(funcs: ITContent<V>): IndexTree<V> {
  // An index tree is never empty. We initialize a new tree with a single leaf node.
  let leaves: Leaves<V> = {
    bounds: [],
    values: [],
    next_leaves: [],
    parents: []
  }
  pushRootLeaf<V>(leaves, funcs)

  return {
    leaves,
    nodes: {
      keys: [],
      child_indexes: [],
      parents: [],
    },

    height: 0,
    root: 0, // The root node.

    // The cached cursor points to the start of the first element.
    cursor: {
      leaf_idx: 0,
      elem_idx: 0
    },
    cursor_key: 0,

    content_funcs: funcs,

    [Symbol.iterator]() {
      return iter(this)
    }
  }
}

export function it_clear<V>(tree: IndexTree<V>) {
  tree.leaves.bounds.length = 0
  tree.leaves.values.length = 0
  tree.leaves.next_leaves.length = 0
  tree.leaves.parents.length = 0
  pushRootLeaf(tree.leaves, tree.content_funcs)

  tree.nodes.keys.length = 0
  tree.nodes.child_indexes.length = 0
  tree.nodes.parents.length = 0

  tree.height = 0
  tree.root = 0
  tree.cursor.leaf_idx = 0
  tree.cursor.elem_idx = 0
  tree.cursor_key = 0

  // Restore the first, empty leaf.
  pushLeaf(tree.leaves)
}



function leaf_has_space<V>(leaves: Leaves<V>, leaf_idx: number, space_wanted: number): boolean {
  if (space_wanted === 0) return true
  return leaves.bounds[LEAF_CHILDREN * (leaf_idx + 1) - space_wanted] === MAX_BOUND
}

function leaf_is_last<V>(leaves: Leaves<V>, leaf_idx: number): boolean {
  return leaves.next_leaves[leaf_idx] === NULL_IDX
}

function remove_from_array<T>(arr: T[], start: number, end: number, arr_end: number) {
  arr.copyWithin(start, end, arr_end)
}

function remove_from_array_2<T>(arr: T[], idx: number, span: number, start: number, end: number) {
  let base = idx * span
  arr.copyWithin(base + start, base + end, base + span)
}

function remove_from_array_fill<T>(arr: T[], start: number, end: number, arr_end: number, fill_val: T) {
  arr.copyWithin(start, end, arr_end)
  arr.fill(fill_val, arr_end - (end - start), arr_end)
}

function remove_from_array_fill_2<T>(arr: T[], idx: number, span: number, start: number, end: number, fill_val: T) {
  let base = idx * span
  arr.copyWithin(base + start, base + end, base + span)
  let arr_end = base + span
  arr.fill(fill_val, arr_end - (end - start), arr_end)
}

function leaf_remove_children<V>(leaves: Leaves<V>, leaf_idx: number, del_start: number, del_end: number) {
  let base = LEAF_CHILDREN * leaf_idx

  remove_from_array_fill_2(leaves.bounds, leaf_idx, LEAF_CHILDREN, del_start, del_end, MAX_BOUND)
  remove_from_array_2(leaves.values, leaf_idx, LEAF_CHILDREN, del_start, del_end)
  // remove_from_array_fill(leaves.bounds, base + del_start, base + del_end, base + LEAF_CHILDREN, MAX_BOUND)
  // remove_from_array(leaves.values, base + del_start, base + del_end, base + LEAF_CHILDREN)
}




function node_is_full(nodes: Nodes, idx: number): boolean {
  return nodes.child_indexes[NODE_CHILDREN * idx] - 1 != NULL_IDX
}

function node_remove_children(nodes: Nodes, idx: number, del_start: number, del_end: number) {
  remove_from_array_fill_2(nodes.keys, idx, NODE_CHILDREN, del_start, del_end, MAX_BOUND)
  remove_from_array_fill_2(nodes.child_indexes, idx, NODE_CHILDREN, del_start, del_end, NULL_IDX)
}

/*

/// I'm not sure if this is a good idea. The index stores its base positions separate from the
/// content.
///
/// Essentially index content must splitable & mergable be such that .truncate() / .append() are
/// no-ops. .can_append will also need the base & offset.
// pub trait IndexContent: Debug + Copy + Eq {
pub trait IndexContent: Debug + Copy {
    /// Try to append other to self. If possible, self is modified (if necessary) and true is
    /// returned.
    fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool;
    // fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
    //     debug_assert!(offset > 0);
    //     debug_assert!(other_len > 0);
    //     &self.at_offset(offset) == other
    // }

    fn at_offset(&self, offset: usize) -> Self;

    fn eq(&self, other: &Self, upto_len: usize) -> bool;
}
*/

function create_new_root_node<V>(tree: IndexTree<V>, lower_bound: LV, child_a: number, split_point: LV, child_b: number): NodeIdx {
  tree.height++
  const new_root_idx = pushNode(tree.nodes)
  const i = new_root_idx * NODE_CHILDREN
  tree.nodes.keys[i] = lower_bound
  tree.nodes.child_indexes[i] = child_a

  tree.nodes.keys[i + 1] = split_point
  tree.nodes.child_indexes[i + 1] = child_b

  tree.root = new_root_idx
  return new_root_idx
}

/** This method always splits a node in the middle. This isn't always optimal, but its simpler. */
function split_node<V>(tree: IndexTree<V>, old_idx: NodeIdx, children_are_leaves: boolean): NodeIdx {
  // Split a full internal node into 2 nodes.
  // let new_node_idx = tree.nodes.parents.length
  const new_node_idx = pushNode(tree.nodes)
  const old_base = old_idx * NODE_CHILDREN
  const new_base = new_node_idx * NODE_CHILDREN

  // println!("split node -> {new_node_idx}");
  // let old_node = &mut self.nodes[old_idx.0];
  // let split_lv = old_node.children[NODE_SPLIT_POINT].0;
  const split_lv = tree.nodes.keys[old_base + NODE_SPLIT_POINT]

  // The old leaf must be full before we split it.
  assert(node_is_full(tree.nodes, old_idx))

  // eprintln!("split node {:?} -> {:?} + {:?} (leaves: {children_are_leaves})", old_idx, old_idx, new_node_idx);
  // eprintln!("split start {:?} / {:?}", &old_node.children[..NODE_SPLIT_POINT], &old_node.children[NODE_SPLIT_POINT..]);

  // Copy old_node[split..] to new_node[0..split].
  tree.nodes.keys.copyWithin(new_base, old_base + NODE_SPLIT_POINT, old_base + NODE_CHILDREN)
  tree.nodes.child_indexes.copyWithin(new_base, old_base + NODE_SPLIT_POINT, old_base + NODE_CHILDREN)
  // new_node.children[0..NODE_SPLIT_POINT].copy_from_slice(&old_node.children[NODE_SPLIT_POINT..]);


  // Clear old_node[split..].
  tree.nodes.keys.fill(MAX_BOUND, old_base + NODE_SPLIT_POINT, old_base + NODE_CHILDREN)
  tree.nodes.child_indexes.fill(MAX_BOUND, old_base + NODE_SPLIT_POINT, old_base + NODE_CHILDREN)
  // old_node.children[NODE_SPLIT_POINT..].fill(EMPTY_NODE_CHILD);

  // Update parent pointer for our children.
  for (let i = 0; i < NODE_SPLIT_POINT; i++) {
    let leaf_idx = tree.nodes.child_indexes[new_base + i]
    ;(children_are_leaves
      ? tree.leaves.parents
      : tree.nodes.parents
    )[leaf_idx] = new_node_idx
  }

  // Insert the new node into the parent node.
  if (old_idx == tree.root) {
    let lower_bound = tree.nodes.keys[old_base]

    // We'll make a new root.
    const parent = create_new_root_node(tree, lower_bound, old_idx, split_lv, new_node_idx)
    tree.nodes.parents[old_idx] = parent
    tree.nodes.parents[new_node_idx] = parent
  } else {
    let parent = tree.nodes.parents[old_idx]
    // Recursively insert.
    tree.nodes.parents[new_node_idx] = insert_into_node(tree, parent, split_lv, new_node_idx, old_idx, false)
  }

  // debug_assert_eq!(new_node_idx, self.nodes.len());

  return new_node_idx
}

function insert_into_node<V>(tree: IndexTree<V>, node_idx: NodeIdx, new_child_key: LV, new_child_idx: number, after_child: number, children_are_leaves: boolean): NodeIdx {
  // let mut node = &mut self[node_idx];

  // Where will the child go? I wonder if the compiler can do anything smart with this...
  let insert_pos = tree.nodes.child_indexes.indexOf(after_child) + 1
  assert(insert_pos > 0) // indexOf returns -1 if the item is not found. In that case, throw.

  // println!("insert_into_node n={:?} after_child {after_child} pos {insert_pos}, new_child {:?}", node_idx, new_child);

  if (node_is_full(tree.nodes, node_idx)) {
    let new_node_idx = split_node(tree, node_idx, children_are_leaves)
    if (insert_pos >= NODE_SPLIT_POINT) {
      // We're inserting into the new node.
      insert_pos -= NODE_SPLIT_POINT
      node_idx = new_node_idx
    }
  }

  // Could scan to find the actual length of the children, then only memcpy that many. But
  // memcpy is cheap.
  let base = node_idx * NODE_CHILDREN
  // node.children.copy_within(insert_pos..NODE_CHILDREN - 1, insert_pos + 1);
  tree.nodes.keys.copyWithin(base + insert_pos + 1, base + insert_pos, base + NODE_CHILDREN - 1)
  tree.nodes.child_indexes.copyWithin(base + insert_pos + 1, base + insert_pos, base + NODE_CHILDREN - 1)

  tree.nodes.keys[base + insert_pos] = new_child_key
  tree.nodes.child_indexes[base + insert_pos] = new_child_idx

  if (insert_pos === 0) {
    let parent = tree.nodes.parents[node_idx]
    recursively_update_nodes(tree, parent, node_idx, new_child_idx);
  }

  return node_idx
}

/**
 * This function splits a full leaf node in the middle, into 2 new nodes.
 * The result is two nodes - old_leaf with items 0..N/2 and new_leaf with items N/2..N.

 * Returns the index of the new leaf.
 */
function split_leaf<V>(tree: IndexTree<V>, old_idx: LeafIdx): LeafIdx {
  let old_height = tree.height
  let leaves = tree.leaves

  let new_leaf_idx = pushLeaf(leaves)
  // let mut old_leaf = &mut self.leaves[old_idx.0];
  // debug_assert!(old_leaf.is_full());
  assert(!leaf_has_space(leaves, old_idx, 2))

  // let parent = old_leaf.parent;
  // let split_lv = old_leaf.bounds[LEAF_SPLIT_POINT];
  let old_base = old_idx * LEAF_CHILDREN
  let split_lv = leaves.bounds[old_base + LEAF_SPLIT_POINT]

  let parent: NodeIdx
  if (old_height == 0) {
      // Insert this leaf into a new root node. This has to be the first node.
      let lower_bound = leaves.bounds[old_base]
      parent = create_new_root_node(tree, lower_bound, old_idx, split_lv, new_leaf_idx)
      // old_leaf = &mut self.leaves[old_idx.0];
      assertEq(parent, 0)
      // let parent = NodeIdx(self.nodes.len());
      leaves.parents[old_idx] = 0
      // debug_assert_eq!(old_leaf.parent, NodeIdx(0)); // Ok because its the default.
      // old_leaf.parent = NodeIdx(0); // Could just default nodes to have a parent of 0.
  } else {
      let old_parent = leaves.parents[old_idx]
      // The parent may change by calling insert_into_node - since the node we're inserting
      // into may split off.

      parent = insert_into_node(tree, old_parent, split_lv, new_leaf_idx, old_idx, true)
  }

  // The old leaf must be full before we split it.
  // debug_assert!(old_leaf.data.last().unwrap().is_some());

  leaves.next_leaves[new_leaf_idx] = leaves.next_leaves[old_idx]
  leaves.parents[new_leaf_idx] = parent

  // We'll steal the second half of the items in OLD_LEAF.
  // new_leaf.children[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.children[LEAF_SPLIT_POINT..]);
  // new_leaf.bounds[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.bounds[LEAF_SPLIT_POINT..]);

  let new_base = new_leaf_idx * LEAF_CHILDREN
  leaves.values.copyWithin(new_base, old_base + LEAF_SPLIT_POINT, old_base + LEAF_CHILDREN)
  leaves.bounds.copyWithin(new_base, old_base + LEAF_SPLIT_POINT, old_base + LEAF_CHILDREN)
  leaves.bounds.fill(MAX_BOUND, old_base + LEAF_SPLIT_POINT, old_base + LEAF_CHILDREN)

  // old_leaf.upper_bound = split_lv;
  leaves.next_leaves[old_idx] = new_leaf_idx

  return new_leaf_idx
}


function make_space_in_leaf_for<V>(tree: IndexTree<V>, leaf_idx: LeafIdx, elem_idx: number, space_wanted: number): [LeafIdx, number] {
  assert(space_wanted === 1 || space_wanted === 2)

  if (!leaf_has_space(tree.leaves, leaf_idx, space_wanted)) {
    let new_leaf_idx = split_leaf(tree, leaf_idx)
    if (elem_idx >= LEAF_SPLIT_POINT) {
      // Inserting into the newly created leaf.
      leaf_idx = new_leaf_idx
      elem_idx -= LEAF_SPLIT_POINT
    }
  }

  let base = leaf_idx * LEAF_CHILDREN
  tree.leaves.bounds.copyWithin(base + elem_idx + space_wanted, base + elem_idx, base + LEAF_CHILDREN - space_wanted)
  tree.leaves.values.copyWithin(base + elem_idx + space_wanted, base + elem_idx, base + LEAF_CHILDREN - space_wanted)

  return [leaf_idx, elem_idx]
}

// Helper function to find LV in a node
function find_lv_in_node(nodes: Nodes, node_idx: NodeIdx, needle: LV): number {
  const base = node_idx * NODE_CHILDREN
  for (let i = 1; i < NODE_CHILDREN; i++) {
    if (needle < nodes.keys[base + i]) return i - 1
  }
  return NODE_CHILDREN - 1
}

// Helper function to find child index in a node
function find_child_idx_in_node(nodes: Nodes, node_idx: NodeIdx, child: number): number {
  const base = node_idx * NODE_CHILDREN;
  let idx = nodes.child_indexes.indexOf(child, node_idx * NODE_CHILDREN)
  assert(idx >= base && idx < base + NODE_CHILDREN)
  return idx
}

function find_in_leaf<V>(leaves: Leaves<V>, leaf_idx: LeafIdx, needle: LV): number {
  // Find the index of the first item where the needle is *not* in the range, and then return
  // the previous item.

  const base = leaf_idx * LEAF_CHILDREN;
  for (let i = 1; i < LEAF_CHILDREN; i++) {
    let b = leaves.bounds[base + i]
    if (b === MAX_BOUND || needle < b) return i - 1
  }
  return LEAF_CHILDREN - 1
}

function leaf_upper_bound<V>(leaves: Leaves<V>, leaf_idx: LeafIdx): LV {
  let next_leaf_idx = leaves.next_leaves[leaf_idx]

  return next_leaf_idx === NULL_IDX
    ? MAX_BOUND
    : leaves.bounds[next_leaf_idx * LEAF_CHILDREN]
}


// *** Cursor functions ***

// Helper function to check that the cursor is at some specified position.
function check_cursor_at<V>(tree: IndexTree<V>, cursor: IndexCursor, lv: LV, at_end: boolean): void {
  DEV: {
    const leaf_base = cursor.leaf_idx * LEAF_CHILDREN
    const lower_bound = tree.leaves.bounds[leaf_base + cursor.elem_idx]

    const next = cursor.elem_idx + 1
    const upper_bound = next < LEAF_CHILDREN && tree.leaves.bounds[leaf_base + next] !== MAX_BOUND
      ? tree.leaves.bounds[leaf_base + next]
      : leaf_upper_bound(tree.leaves, cursor.leaf_idx)

    assert(lv >= lower_bound);

    if (at_end) {
      assertEq(lv, upper_bound);
    } else {
      assert(lv < upper_bound, `Cursor is not within expected bound. Expect ${lv} / upper_bound ${upper_bound}`);
    }
  }
}

function cursor_to_next<V>(tree: IndexTree<V>, cursor: IndexCursor) {
  const leaf_base = cursor.leaf_idx * LEAF_CHILDREN;
  const next_idx = cursor.elem_idx + 1;

  if (next_idx >= LEAF_CHILDREN || tree.leaves.bounds[leaf_base + next_idx] === MAX_BOUND) {
    // Move the cursor to the next leaf node.
    cursor.leaf_idx = tree.leaves.next_leaves[cursor.leaf_idx]
    cursor.elem_idx = 0
  } else {
    cursor.elem_idx++
  }
}

// Generate a cursor which points at the specified LV.
function cursor_at<V>(tree: IndexTree<V>, lv: LV): IndexCursor {
  assert(lv < MAX_BOUND)

  if (tree.cursor_key === lv) {
    DEV: check_cursor_at(tree, tree.cursor, lv, false)

    // TODO: Consider cloning cursor here.
    // return tree.cursor
    return { // TODO: Do we really need to deep clone here?
      leaf_idx: tree.cursor.leaf_idx,
      elem_idx: tree.cursor.elem_idx,
    }
  }

  const leaf_base = tree.cursor.leaf_idx * LEAF_CHILDREN
  if (lv >= tree.leaves.bounds[leaf_base]) {
    // There are 3 cases:
    // - The lv is less than the bound (or this is the last node)
    // - The lv is exactly the same as the upper bound. Use the start of the next leaf
    // - Or the LV is something else. Scan normally.

    const upper_bound = leaf_upper_bound(tree.leaves, tree.cursor.leaf_idx)
    if (lv < upper_bound) {
      return {
        leaf_idx: tree.cursor.leaf_idx,
        elem_idx: find_in_leaf(tree.leaves, tree.cursor.leaf_idx, lv)
      }
    } else if (lv === upper_bound) {
      return {
        leaf_idx: tree.leaves.next_leaves[tree.cursor.leaf_idx],
        elem_idx: 0, // has to be.
      }
    }
  }

  // Make a cursor by descending from the root.
  let idx = tree.root;
  for (let h = 0; h < tree.height; h++) {
    const slot = find_lv_in_node(tree.nodes, idx, lv);
    idx = tree.nodes.child_indexes[idx * NODE_CHILDREN + slot];
  }

  return {
    leaf_idx: idx,
    elem_idx: find_in_leaf(tree.leaves, idx, lv)
  };
}

export interface RleDRun<V> {
  start: number,
  end: number,
  val: V,
}

/// Get the entry at the specified offset. This will return the largest run of values which
/// contains the specified index.
export function it_get_entry<V>(tree: IndexTree<V>, lv: LV): RleDRun<V> {
  const cursor = cursor_at(tree, lv);

  DEV: check_cursor_at(tree, cursor, lv, false);

  // TODO: Is this needed?
  tree.cursor = cursor;
  tree.cursor_key = lv;

  const leaf_base = cursor.leaf_idx * LEAF_CHILDREN;
  const val = tree.leaves.values[leaf_base + cursor.elem_idx];
  const lower_bound = tree.leaves.bounds[leaf_base + cursor.elem_idx];

  const next_elem = cursor.elem_idx + 1;
  const upper_bound = next_elem >= LEAF_CHILDREN || tree.leaves.bounds[leaf_base + next_elem] === MAX_BOUND
    ? leaf_upper_bound(tree.leaves, cursor.leaf_idx)
    : tree.leaves.bounds[leaf_base + next_elem]

  assert(lv >= lower_bound && lv < upper_bound);

  return {
    start: lower_bound,
    end: upper_bound,
    val
  }
}

// After the first item in a leaf has been modified, we need to walk up the node tree to update
// the start LV values.
function recursively_update_nodes<V>(tree: IndexTree<V>, node_idx: NodeIdx, child: number, new_start: LV): void {
  while (node_idx !== NULL_IDX) {
    const node_base = node_idx * NODE_CHILDREN
    const child_idx = find_child_idx_in_node(tree.nodes, node_idx, child);
    tree.nodes.keys[node_base + child_idx] = new_start
    if (child_idx !== 0) {
      // We're done here. This is the most likely case.
      break;
    }

    // Otherwise continue up the tree until we hit the root.
    child = node_idx
    node_idx = tree.nodes.parents[node_idx]
  }
}

// function get_leaf_and_bound<V>(tree: IndexTree<V>, idx: LeafIdx): [Leaves<V>, LV] {
//   const upper_bound = leaf_upper_bound(tree.leaves, idx);
//   return [tree.leaves, upper_bound];
// }


// Helper function to trim leaf end
// Returns true if we need to keep trimming stuff after this leaf.
function trim_leaf_end<V>(tree: IndexTree<V>, leaf_idx: LeafIdx, elem_idx: number, end: LV): boolean {
  assert(elem_idx >= 1);
  let leaves = tree.leaves
  let upper_bound = leaf_upper_bound(leaves, leaf_idx)
  const leaf_base = leaf_idx * LEAF_CHILDREN;

  DEV: {
    // Check the bounds monotonically increase
    let prev = leaves.bounds[leaf_base]
    for (let i = 1; i < elem_idx; i++) {
      const b = leaves.bounds[leaf_base + i];
      if (b !== MAX_BOUND) {
        assert(b > prev, `Bounds does not monotonically increase b=${leaves.bounds}`);
      }
      prev = b
    }
  }

  if (elem_idx >= LEAF_CHILDREN || leaves.bounds[leaf_base + elem_idx] === MAX_BOUND) {
    // The cat is already out of the bag. Continue trimming after this leaf.
    return end > upper_bound;
  }

  let del_to = elem_idx

  while (del_to < LEAF_CHILDREN) {
    const next_idx = del_to + 1;
    assert(next_idx <= LEAF_CHILDREN)

    // The bounds of the next element.
    let b = next_idx === LEAF_CHILDREN
      ? upper_bound
      : leaves.bounds[leaf_base + next_idx]

    if (b === MAX_BOUND) b = upper_bound

    if (end < b) {
      assert(leaves.bounds[leaf_base + del_to] < end)

      leaves.values[leaf_base + del_to] = tree.content_funcs.at_offset(
        leaves.values[leaf_base + del_to],
        end - leaves.bounds[leaf_base + del_to]
      )
      leaves.bounds[leaf_base + del_to] = end
      break

    } else if (end === b) {
      // The current item is the last item to delete.
      del_to++
      break
    } else {
      // Keep scanning.
      del_to++;
    }

    // Bleh!
    if (next_idx < LEAF_CHILDREN && leaves.bounds[leaf_base + next_idx] === MAX_BOUND) {
      break;
    }
  }

  if (del_to >= LEAF_CHILDREN || leaves.bounds[leaf_base + del_to] === MAX_BOUND) {
    // Delete the rest of this leaf and bubble up.
    leaves.bounds.fill(MAX_BOUND, leaf_base + elem_idx, leaf_base + LEAF_CHILDREN);
    return end > upper_bound;
  } else {
    const trimmed_items = del_to - elem_idx;

    if (trimmed_items >= 1) {
      leaf_remove_children(leaves, leaf_idx, elem_idx, del_to);
    }
    return false;
  }
}


function upper_bound_scan<V>(tree: IndexTree<V>, idx: number, height: number): number {
  while (height > 0) {
    // Descend to the last child of this item.
    const node_base = idx * NODE_CHILDREN
    assertNe(tree.nodes.child_indexes[node_base], MAX_BOUND, `Node is empty. idx: ${idx}`);

    // Find the last child. This is a bit convoluted, but it looks right.
    let last_child_idx = -1
    for (let i = NODE_CHILDREN - 1; i >= 0; i--) {
      if (tree.nodes.child_indexes[node_base + i] !== NULL_IDX) {
        last_child_idx = tree.nodes.child_indexes[node_base + i];
        break;
      }
    }
    assertNe(last_child_idx, -1, "Invalid state: Node is empty");

    height--;
    idx = last_child_idx;
  }

  // idx is now pointing to a leaf.
  return leaf_upper_bound(tree.leaves, idx);
}

function trim_node_start<V>(tree: IndexTree<V>, idx: number, end: LV, height: number): LeafIdx {
  while (height > 0) {
    const node_base = idx * NODE_CHILDREN

    if (end > tree.nodes.keys[node_base]) {
      const keep_child_idx = find_lv_in_node(tree.nodes, idx, end)

      DEV: {
        const i = tree.nodes.child_indexes[node_base + keep_child_idx]
        assert(upper_bound_scan(tree, i, height - 1) > end)
      }

      if (keep_child_idx >= 1) {
        // In this case, we're leaking nodes. This happens so rarely in practice that I'm not
        // worried about it.
        node_remove_children(tree.nodes, idx, 0, keep_child_idx)
      }

      tree.nodes.keys[node_base] = end
    }

    idx = tree.nodes.child_indexes[node_base];
    height--;
  }

  // Ok, now drop the first however many items from the leaf.
  const leaf_base = idx * LEAF_CHILDREN
  const keep_elem_idx = find_in_leaf(tree.leaves, idx, end);
  if (keep_elem_idx >= 1) {
    leaf_remove_children(tree.leaves, idx, 0, keep_elem_idx);
  }
  tree.leaves.values[leaf_base] = tree.content_funcs.at_offset(
    tree.leaves.values[leaf_base],
    end - tree.leaves.bounds[leaf_base]
  )
  tree.leaves.bounds[leaf_base] = end

  DEV: assert(leaf_upper_bound(tree.leaves, idx) >= end)

  return idx
}

function trim_node_end_after_child<V>(tree: IndexTree<V>, node_idx: NodeIdx, child: number, end: LV, height: number): LeafIdx {
  assert(height >= 1);

  const node_base = node_idx * NODE_CHILDREN;
  const idx = find_child_idx_in_node(tree.nodes, node_idx, child);

  const del_start = idx + 1;

  DEV: {
    const child_idx = tree.nodes.child_indexes[node_base + idx];
    const up = upper_bound_scan(tree, child_idx, height - 1);
    assert(end > up);
    if (del_start < NODE_CHILDREN && tree.nodes.child_indexes[node_base + del_start] !== NULL_IDX) {
      assert(end > up);
    }
  }

  for (let i = del_start; i < NODE_CHILDREN; i++) {
    const child_idx = tree.nodes.child_indexes[node_base + i];

    if (child_idx === NULL_IDX) break;

    const upper_bound = i + 1 < NODE_CHILDREN && tree.nodes.child_indexes[node_base + i + 1] !== NULL_IDX
      ? tree.nodes.keys[node_base + i + 1]
      : upper_bound_scan(tree, child_idx, height - 1);

    if (end < upper_bound) {
      tree.nodes.keys[node_base + i] = end;

      if (i > del_start) {
        node_remove_children(tree.nodes, node_idx, del_start, i);
      }

      return trim_node_start(tree, child_idx, end, height - 1);
    }
  }

  tree.nodes.child_indexes.fill(NULL_IDX, node_base + del_start, node_base + NODE_CHILDREN)
  tree.nodes.keys.fill(MAX_BOUND, node_base + del_start, node_base + NODE_CHILDREN)

  assertNe(tree.nodes.parents[node_idx], NULL_IDX, "Invalid bounds")
  const parent = tree.nodes.parents[node_idx]
  return trim_node_end_after_child(tree, parent, node_idx, end, height + 1)
}

// This method clears everything out of the way for the specified element, to set its
// upper bound correctly.
function extend_upper_range<V>(tree: IndexTree<V>, leaf_idx: LeafIdx, elem_idx: number, end: LV): void {
  // This may need to do a lot of work:
  // - The leaf we're currently inside of needs to be trimmed, from elem_idx onwards
  // - If we continue, the parent leaf needs to be trimmed, and its parent and so on. This may
  //   cause some leaves and nodes to be discarded entirely.
  // - Then some nodes and a leaf may need the first few elements removed.

  // We'll always call this with the "next" elem_idx. So the leaf thats being trimmed will
  // never itself be removed.
  assert(elem_idx >= 1);

  // First, trim the end of this leaf if we can.
  if (trim_leaf_end(tree, leaf_idx, elem_idx, end) && tree.height > 0) {
    const parent = tree.leaves.parents[leaf_idx];
    assertNe(parent, NULL_IDX);

    const new_next_leaf = trim_node_end_after_child(tree, parent, leaf_idx, end, 1);
    tree.leaves.next_leaves[leaf_idx] = new_next_leaf;
  }
}

export function it_set_range<V>(tree: IndexTree<V>, start: number, end: number, data: V): void {
  if (start === end) return;
  const cursor = cursor_at(tree, start);

  DEV: check_cursor_at(tree, cursor, start, false);

  // The cursor may move.
  const [new_cursor, at_end] = set_range_internal(tree, cursor, start, end, data);

  DEV: check_cursor_at(tree, new_cursor, end, at_end);

  if (at_end) {
    cursor_to_next(tree, new_cursor);
    DEV: check_cursor_at(tree, new_cursor, end, false);
  }
  tree.cursor = new_cursor;
  tree.cursor_key = end;
}


function set_range_internal<V>(tree: IndexTree<V>, cursor: IndexCursor, start: number, end: number, data: V): [IndexCursor, boolean] {
  let { leaf_idx, elem_idx } = cursor

  let leaves = tree.leaves
  let l_upper_bound = leaf_upper_bound(leaves, leaf_idx)
  const leaf_base = leaf_idx * LEAF_CHILDREN;

  assertNe(leaves.bounds[leaf_base + elem_idx], MAX_BOUND);
  assert(start >= leaves.bounds[leaf_base] || leaf_idx === 0);
  assert(start < l_upper_bound);

  assert(elem_idx < LEAF_CHILDREN);

  let cur_start = leaves.bounds[leaf_base + elem_idx];

  const { at_offset, try_append } = tree.content_funcs

  if (cur_start === start && elem_idx > 0) {
    // Try and append it to the previous item. This is strictly unnecessary, but should help with
    // perf.
    const prev_idx = elem_idx - 1;
    const prev_start = leaves.bounds[leaf_base + prev_idx];
    if (try_append(leaves.values[leaf_base + prev_idx], cur_start - prev_start, data, end - start)) {
      // Ok!
      extend_upper_range(tree, leaf_idx, elem_idx, end);

      // Note extend_upper_range might have nuked the current element.
      leaves = tree.leaves;
      if (leaves.bounds[leaf_base + elem_idx] === MAX_BOUND) {
        return [{ leaf_idx: leaves.next_leaves[leaf_idx], elem_idx: 0 }, false];
      } else {
        return [cursor, false];
      }
    }
  }

  // TODO: Probably worth a short-circuit check here to see if the value even changed.

  let cur_end = elem_idx >= LEAF_CHILDREN - 1
    ? l_upper_bound
    : leaves.bounds[leaf_base + elem_idx + 1] === MAX_BOUND
      ? l_upper_bound
      : leaves.bounds[leaf_base + elem_idx + 1];

  // If we can append the item to the current item, do that.
  if (cur_start < start) {
    let d = leaves.values[leaf_base + elem_idx];
    if (try_append(d, start - cur_start, data, end - start)) {
      data = d
      start = cur_start
    }
  }

  let end_is_end = true

  if (end < cur_end) {
    // Try to append the end of the current element.
    assertNe(leaves.bounds[leaf_base + elem_idx], MAX_BOUND)
    let rem = at_offset(leaves.values[leaf_base + elem_idx], end - start)
    if (try_append(data, end - start, rem, cur_end - end)) {
      // Nice. We'll handle this in the special case below.
      end = cur_end;
      end_is_end = false;
    } else {
      // In this case, the item is replacing a prefix of the target slot. We'll just hardcode
      // these cases, since otherwise we need to deal with remainders below and thats a pain.
      if (cur_start < start) {
        // We need to "splice in" this item. Eg, x -> xyx. This will result in 2
        // inserted items.

        // The resulting behaviour should be that:
        // b1 (x) b2  ---->  b1 (x) start (y) range.end (x) b2

        // The item at elem_idx is the start of the item we're splitting. Leave it
        // alone. We'll replace elem_idx + 1 with data and elem_idx + 2 with remainder.
        [leaf_idx, elem_idx] = make_space_in_leaf_for(tree, leaf_idx, elem_idx, 2);
        const new_leaf_base = leaf_idx * LEAF_CHILDREN;

        assert(elem_idx + 2 < LEAF_CHILDREN)
        leaves.bounds[new_leaf_base + elem_idx + 1] = start;
        leaves.values[new_leaf_base + elem_idx + 1] = data;
        leaves.bounds[new_leaf_base + elem_idx + 2] = end;
        leaves.values[new_leaf_base + elem_idx + 2] = at_offset(leaves.values[new_leaf_base + elem_idx], end - cur_start);

        // We modified elem_idx +1 and +2, so we can't have modified index 0. No parent update.
        return [{ leaf_idx, elem_idx: elem_idx + 1 }, true];
      } else {
        // Preserve the end of this item. Eg, x -> yx.
        assertEq(cur_start, start);
        assert(end < cur_end);

        [leaf_idx, elem_idx] = make_space_in_leaf_for(tree, leaf_idx, elem_idx, 1);
        const new_leaf_base = leaf_idx * LEAF_CHILDREN;

        assertEq(leaves.bounds[new_leaf_base + elem_idx], start);
        assert(elem_idx + 1 < LEAF_CHILDREN);
        leaves.values[new_leaf_base + elem_idx] = data;
        leaves.bounds[new_leaf_base + elem_idx + 1] = end;
        leaves.values[new_leaf_base + elem_idx + 1] = at_offset(leaves.values[new_leaf_base + elem_idx + 1], end - start);

        // Since start == lower bound, the parents won't need updating.
        return [{ leaf_idx, elem_idx }, true];
      }
    }
  }

  if (end === cur_end) {
    // Special case. Might not be worth it.
    if (start === cur_start) {
      // Nuke the existing item.
      leaves.values[leaf_base + elem_idx] = data;
    } else {
      // Preserve the start of the item. x -> xy.
      assert(start > cur_start);

      [leaf_idx, elem_idx] = make_space_in_leaf_for(tree, leaf_idx, elem_idx, 1);
      const new_leaf_base = leaf_idx * LEAF_CHILDREN;

      elem_idx++
      assert(elem_idx < LEAF_CHILDREN);
      leaves.values[new_leaf_base + elem_idx] = data;
      leaves.bounds[new_leaf_base + elem_idx] = start;
    }
    return [{ leaf_idx, elem_idx }, end_is_end];
  }

  // This element overlaps with some other elements.
  assert(end > cur_end);
  assert(start < cur_end);

  if (cur_start < start) {
    // Trim the current item alone and modify the next item.
    // If we get here then: cur_start < start < cur_end < end.
    assert(cur_start < start && start < cur_end && cur_end < end);

    elem_idx++

    if (elem_idx >= LEAF_CHILDREN || leaves.bounds[leaf_base + elem_idx] === MAX_BOUND) {
      // This is the end of the leaf node.
      if (leaf_is_last(leaves, leaf_idx)) {
        throw new Error("I don't think this can happen");
      } else {
        // We've trimmed this leaf node. Roll the cursor to the next item.
        leaf_idx = leaves.next_leaves[leaf_idx];
        l_upper_bound = leaf_upper_bound(leaves, leaf_idx);
        elem_idx = 0;

        // We're going to replace the leaf's starting item.
        recursively_update_nodes(tree, leaves.parents[leaf_idx], leaf_idx, start);
      }
    }

    assertEq(leaves.bounds[leaf_base + elem_idx], cur_end);
    assert(start < leaves.bounds[leaf_base + elem_idx]);

    // Right now leaf.children[elem_idx] contains an item from cur_end > start.

    // We've moved forward. Try and append the existing item to data.
    cur_start = cur_end;
    cur_end = elem_idx >= LEAF_CHILDREN - 1
      ? l_upper_bound
      : leaves.bounds[leaf_base + elem_idx + 1] === MAX_BOUND
        ? l_upper_bound
        : leaves.bounds[leaf_base + elem_idx + 1];

    leaves.bounds[leaf_base + elem_idx] = start;

    assert(start < cur_start && cur_start < cur_end);
    assert(cur_start < end);

    if (end < cur_end) {
      // Try to prepend the new item to the start of the existing item.
      if (try_append(data, cur_start - start, leaves.values[leaf_base + elem_idx], cur_end - cur_start)) {
        // Ok!
        leaves.values[leaf_base + elem_idx] = data;
        return [{ leaf_idx, elem_idx }, false];
      } else {
        [leaf_idx, elem_idx] = make_space_in_leaf_for(tree, leaf_idx, elem_idx, 1);
        const new_leaf_base = leaf_idx * LEAF_CHILDREN;
        leaves.values[new_leaf_base + elem_idx] = data;
        leaves.bounds[new_leaf_base + elem_idx + 1] = end;
        leaves.values[new_leaf_base + elem_idx + 1] = at_offset(leaves.values[new_leaf_base + elem_idx + 1], end - cur_start);
        return [{ leaf_idx, elem_idx }, end_is_end];
      }
    } else if (end === cur_end) {
      // This item fits perfectly.
      leaves.values[leaf_base + elem_idx] = data;
      return [{ leaf_idx, elem_idx }, end_is_end];
    }

    cur_start = start; // Since we've pushed down the item bounds.
  }

  assert(end > cur_end);
  assertEq(cur_start, start);

  // We don't care about the current element at all. Just overwrite it and extend
  // the bounds.
  leaves.values[leaf_base + elem_idx] = data;
  extend_upper_range(tree, leaf_idx, elem_idx + 1, end);

  return [{ leaf_idx, elem_idx }, end_is_end];
}

function first_leaf<V>(tree: IndexTree<V>): LeafIdx {
  DEV: {
    let idx = tree.root;
    for (let i = 0; i < tree.height; i++) {
      idx = tree.nodes.child_indexes[idx * NODE_CHILDREN];
    }
    assertEq(idx, 0);
  }
  return 0;
}

// export function it_is_empty<V>(tree: IndexTree<V>): boolean {
//   return tree.leaves.bounds[0] === MAX_BOUND;
// }

export function it_count_items<V>(tree: IndexTree<V>): number {
  let count = 0;
  let leaf_idx = first_leaf(tree);
  
  while (true) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;
    count += tree.leaves.bounds.slice(leaf_base, leaf_base + LEAF_CHILDREN)
      .filter(b => b !== MAX_BOUND).length;

    // There is always at least one leaf.
    if (leaf_is_last(tree.leaves, leaf_idx)) break;
    leaf_idx = tree.leaves.next_leaves[leaf_idx];
  }

  return count;
}


function* iter<V>(tree: IndexTree<V>): Generator<RleDRun<V>, void, unknown> {
  const leaves = tree.leaves
  let leaf_idx = first_leaf(tree)
  let prev_val: V | undefined = undefined
  let prev_bound = 0

  while (leaf_idx !== NULL_IDX) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;
    // const start = leaves.bounds[leaf_base + elem_idx];

    for (let i = 0; i < LEAF_CHILDREN; i++) {
      let bound = leaves.bounds[leaf_base + i]
      if (bound == MAX_BOUND) break;

      if (prev_val !== undefined) {
        yield { start: prev_bound, end: bound, val: prev_val }
      }
      prev_bound = bound
      prev_val = leaves.values[leaf_base + i]
    }

    leaf_idx = leaves.next_leaves[leaf_idx]
  }

  if (prev_val !== undefined) {
    yield { start: prev_bound, end: MAX_BOUND, val: prev_val }
  }
}


// *** Debug checking

function dbg_check_walk<V>(tree: IndexTree<V>, idx: number, height: number, expect_start: LV | null, expect_parent: NodeIdx): void {
  if (height > 0) {
    // Visiting a node.
    assert(idx < tree.nodes.parents.length)
    const node_base = idx * NODE_CHILDREN;

    assert(tree.nodes.parents[idx] === expect_parent);

    // The first child must be in use.
    assert(tree.nodes.child_indexes[node_base] !== NULL_IDX);
    // The first child must start at expect_start.
    if (expect_start !== null) {
      assert(tree.nodes.keys[node_base] === expect_start);
    }

    let finished = false;
    let prev_start = MAX_BOUND;
    for (let i = 0; i < NODE_CHILDREN; i++) {
      const start = tree.nodes.keys[node_base + i];
      const child_idx = tree.nodes.child_indexes[node_base + i];
      
      if (child_idx === NULL_IDX) {
        finished = true;
      } else {
        assert(prev_start === MAX_BOUND || prev_start < start, `prev_start ${prev_start} / start ${start}`);
        prev_start = start;

        assert(finished === false);
        dbg_check_walk(tree, child_idx, height - 1, start, idx);
      }
    }
  } else {
    // Visiting a leaf.
    assert(idx < tree.leaves.parents.length);
    const leaf_base = idx * LEAF_CHILDREN;

    assert(tree.leaves.parents[idx] === expect_parent);

    // We check that the first child is in use below.
    if (tree.leaves.bounds[leaf_base] !== MAX_BOUND) {
      if (expect_start !== null) {
        assert(tree.leaves.bounds[leaf_base] === expect_start);
      }
    }
  }
}


function it_dbg_check<V>(tree: IndexTree<V>): void {
  // Invariants:
  // - All index markers point to the node which contains the specified item.
  // - Except for the root item, all leaves must have at least 1 data entry.
  // - The "left edge" of items should all have a lower bound of 0
  // - The last leaf node should have an upper bound and node_next of usize::MAX.

  // This code does 2 traversals of the data structure:
  // 1. We walk the leaves by following next_leaf pointers in each leaf node
  // 2. We recursively walk the tree

  // Walk the leaves.
  let leaves_visited = 0;
  let leaf_idx = first_leaf(tree);
  while (true) {
    const leaf_base = leaf_idx * LEAF_CHILDREN;
    leaves_visited++;

    if (leaf_idx === first_leaf(tree)) {
      // First leaf. This can be empty - but only if the whole data structure is empty.
      if (tree.leaves.bounds[leaf_base] === MAX_BOUND) {
        assertEq(tree.leaves.next_leaves[leaf_idx], NULL_IDX);
      }
    } else {
      assertNe(tree.leaves.bounds[leaf_base], MAX_BOUND, "Only the first leaf can be empty");
    }

    // Make sure the bounds are all sorted.
    let prev = tree.leaves.bounds[leaf_base];
    let finished = false;
    for (let i = 1; i < LEAF_CHILDREN; i++) {
      const b = tree.leaves.bounds[leaf_base + i];
      if (b === MAX_BOUND) {
        finished = true;
      } else {
        assert(b > prev, `Bounds does not monotonically increase b=${tree.leaves.bounds.slice(leaf_base, leaf_base + LEAF_CHILDREN)}`);
        prev = b;
        assert(!finished, "All in-use children must come before all null children");
      }
    }

    if (leaf_is_last(tree.leaves, leaf_idx)) break;
    const next_leaf_base = tree.leaves.next_leaves[leaf_idx] * LEAF_CHILDREN;
    assert(tree.leaves.bounds[next_leaf_base] > prev);
    leaf_idx = tree.leaves.next_leaves[leaf_idx];
  }

  assertEq(leaves_visited, tree.leaves.parents.length);

  if (tree.height === 0) {
    assert(tree.root < tree.leaves.parents.length);
  } else {
    assert(tree.root < tree.nodes.parents.length);
  }

  // And walk the tree structure in the nodes
  dbg_check_walk(tree, tree.root, tree.height, null, NULL_IDX);

  // Check the cursor
  check_cursor_at(tree, tree.cursor, tree.cursor_key, false);
}

/*

    #[allow(unused)]
    pub(crate) fn dbg_check_eq_2(&self, other: impl IntoIterator<Item = RleDRun<V>>) {
        self.dbg_check();

        let mut tree_iter = self.iter();
        // let mut expect_iter = expect.into_iter();

        // while let Some(expect_val) = expect_iter.next() {
        let mut actual_remainder = None;
        for mut expect in other.into_iter() {
            loop {
                let mut actual = actual_remainder.take().unwrap_or_else(|| {
                    tree_iter.next().expect("Tree missing item")
                });

                // Skip anything before start.
                if actual.end <= expect.start {
                    continue;
                }

                // Trim the start of actual_next
                if actual.start < expect.start {
                    (_, actual) = split_rle(actual, expect.start - actual.start);
                } else if expect.start < actual.start {
                    panic!("Missing element");
                }

                assert_eq!(actual.start, expect.start);
                let r = DTRange { start: actual.start, end: actual.start + usize::min(actual.len(), expect.len()) };
                assert!(expect.val.eq(&actual.val, usize::min(actual.len(), expect.len())),
                        "at {:?}: expect {:?} != actual {:?} (len={})", r, &expect.val, &actual.val, usize::min(actual.len(), expect.len()));
                // assert_eq!(expect.val, actual.val, "{:?}", &tree_iter);

                if actual.end > expect.end {
                    // We don't need to split it here because that'll happen on the next iteration anyway.
                    actual_remainder = Some(actual);
                    // actual_remainder = Some(split_rle(actual, expect.end - actual.start).1);
                    break;
                } else if actual.end >= expect.end {
                    break;
                } else {
                    // actual.end < expect.end
                    // Keep the rest of expect for the next iteration.
                    (_, expect) = split_rle(expect, actual.end - expect.start);
                    debug_assert_eq!(expect.start, actual.end);
                    // And continue with this expected item.
                }
            }
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_check_eq<'a>(&self, vals: impl IntoIterator<Item = &'a RleDRun<V>>) where V: 'a {
        self.dbg_check_eq_2(vals.into_iter().copied());
    }

}



#[cfg(test)]
mod test {
    use std::pin::Pin;
    use rand::prelude::SmallRng;
    use rand::{Rng, SeedableRng};
    use content_tree::{ContentTreeRaw, RawPositionMetricsUsize};
    use crate::list_fuzzer_tools::fuzz_multithreaded;
    use super::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum Foo { A, B, C }
    use Foo::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
    struct X(usize);
    impl IndexContent for X {
        fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
            debug_assert!(offset > 0);
            debug_assert!(other_len > 0);
            &self.at_offset(offset) == other
        }

        fn at_offset(&self, offset: usize) -> Self {
            X(self.0 + offset)
        }

        fn eq(&self, other: &Self, _upto_len: usize) -> bool {
            self.0 == other.0
        }
    }

    #[test]
    fn empty_tree_is_empty() {
        let tree = IndexTree::<X>::new();

        tree.dbg_check_eq(&[]);
    }

    #[test]
    fn overlapping_sets() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
        // assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);
        // dbg!(&tree.leaves[0]);
        tree.set_range((5..11).into(), X(200));
        tree.dbg_check_eq(&[RleDRun::new(5..11, X(200))]);

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[
            RleDRun::new(5..10, X(100)),
            RleDRun::new(10..11, X(205)),
        ]);

        tree.set_range((2..50).into(), X(300));
        // dbg!(&tree.leaves);
        tree.dbg_check_eq(&[RleDRun::new(2..50, X(300))]);

    }

    #[test]
    fn split_values() {
        let mut tree = IndexTree::new();
        tree.set_range((10..20).into(), X(100));
        tree.set_range((12..15).into(), X(200));
        tree.dbg_check_eq(&[
            RleDRun::new(10..12, X(100)),
            RleDRun::new(12..15, X(200)),
            RleDRun::new(15..20, X(105)),
        ]);
    }

    #[test]
    fn set_inserts_1() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);

        tree.set_range((5..10).into(), X(200));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(200))]);

        // dbg!(&tree);
        tree.set_range((15..20).into(), X(300));
        // dbg!(tree.iter().collect::<Vec<_>>());
        tree.dbg_check_eq(&[
            RleDRun::new(5..10, X(200)),
            RleDRun::new(15..20, X(300)),
        ]);

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
    }

    #[test]
    fn set_inserts_2() {
        let mut tree = IndexTree::new();
        tree.set_range((5..10).into(), X(100));
        tree.set_range((1..5).into(), X(200));
        // dbg!(&tree);
        tree.dbg_check_eq(&[
            RleDRun::new(1..5, X(200)),
            RleDRun::new(5..10, X(100)),
        ]);
        dbg!(&tree.leaves[0]);

        tree.set_range((3..8).into(), X(300));
        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
        tree.dbg_check_eq(&[
            RleDRun::new(1..3, X(200)),
            RleDRun::new(3..8, X(300)),
            RleDRun::new(8..10, X(103)),
        ]);
    }

    #[test]
    fn split_leaf() {
        let mut tree = IndexTree::new();
        // Using 10, 20, ... so they don't merge.
        tree.set_range(10.into(), X(100));
        tree.dbg_check();
        tree.set_range(20.into(), X(200));
        tree.set_range(30.into(), X(100));
        tree.set_range(40.into(), X(200));
        tree.dbg_check();
        // dbg!(&tree);
        tree.set_range(50.into(), X(100));
        tree.dbg_check();

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());

        tree.dbg_check_eq(&[
            RleDRun::new(10..11, X(100)),
            RleDRun::new(20..21, X(200)),
            RleDRun::new(30..31, X(100)),
            RleDRun::new(40..41, X(200)),
            RleDRun::new(50..51, X(100)),
        ]);
    }

    #[test]
    fn clear_range() {
        // for i in 2..20 {
        for i in 2..50 {
            eprintln!("i: {i}");
            let mut tree = IndexTree::new();
            for base in 0..i {
                tree.set_range((base*3..base*3+2).into(), X(base + 100));
            }
            // dbg!(tree.iter().collect::<Vec<_>>());

            let ceil = i*3 - 2;
            // dbg!(ceil);
            // dbg!(&tree);
            tree.dbg_check();
            tree.set_range((1..ceil).into(), X(99));
            // dbg!(tree.iter().collect::<Vec<_>>());

            tree.dbg_check_eq(&[
                RleDRun::new(0..1, X(100)),
                RleDRun::new(1..ceil, X(99)),
                RleDRun::new(ceil..ceil+1, X(i - 1 + 100 + 1)),
            ]);
        }
    }

    fn fuzz(seed: u64, verbose: bool) {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree = IndexTree::new();
        // let mut check_tree: Pin<Box<ContentTreeRaw<RleDRun<Option<i32>>, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        let mut check_tree: Pin<Box<ContentTreeRaw<DTRange, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        const START_JUNK: usize = 1_000_000;
        check_tree.replace_range_at_offset(0, (START_JUNK..START_JUNK *2).into());

        for _i in 0..1000 {
            if verbose { println!("i: {}", _i); }
            // This will generate some overlapping ranges sometimes but not too many.
            let val = rng.gen_range(0..100) + 100;
            // let start = rng.gen_range(0..3);
            let start = rng.gen_range(0..1000);
            let len = rng.gen_range(0..100) + 1;
            // let start = rng.gen_range(0..100);
            // let len = rng.gen_range(0..100) + 1;

            // dbg!(&tree, start, len, val);
            // if _i == 19 {
            //     println!("blerp");
            // }

            // if _i == 14 {
            //     dbg!(val, start, len);
            //     dbg!(tree.iter().collect::<Vec<_>>());
            // }
            tree.set_range((start..start+len).into(), X(val));
            // dbg!(&tree);
            tree.dbg_check();

            // dbg!(check_tree.iter().collect::<Vec<_>>());

            check_tree.replace_range_at_offset(start, (val..val+len).into());

            // if _i == 14 {
            //     dbg!(tree.iter().collect::<Vec<_>>());
            //     dbg!(check_tree.iter_with_pos().filter_map(|(pos, r)| {
            //         if r.start >= START_JUNK { return None; }
            //         Some(RleDRun::new(pos..pos+r.len(), X(r.start)))
            //     }).collect::<Vec<_>>());
            // }

            // check_tree.iter
            tree.dbg_check_eq_2(check_tree.iter_with_pos().filter_map(|(pos, r)| {
                if r.start >= START_JUNK { return None; }
                Some(RleDRun::new(pos..pos+r.len(), X(r.start)))
            }));
        }
    }

    #[test]
    fn fuzz_once() {
        fuzz(22, true);
    }

    #[test]
    #[ignore]
    fn tree_fuzz_forever() {
        fuzz_multithreaded(u64::MAX, |seed| {
            if seed % 100 == 0 {
                println!("Iteration {}", seed);
            }
            fuzz(seed, false);
        })
    }
}

*/

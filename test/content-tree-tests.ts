
import assert from 'node:assert/strict'
import { ContentTreeFuncs, ct_cursor_at_start, ct_cursor_before_cur_pos, ct_debug_check, ct_emplace_cursor, ct_inc_cursor_offset, ct_insert, ct_iter, ct_iter_leaves, ct_iter_rle, ct_mutate_entry, ct_print_tree, ct_total_cur_len, ct_total_end_len, ctCreate } from '../src/content-tree.js';
import { LeafIdx, LV } from '../src/tree-common.js';
import SeedRandom from "seed-random";

interface TestRange {
  id: number;
  len: number;
  is_activated: boolean;
  exists: boolean;
}


// Create a test utility implementation of ContentTreeFuncs
const TEST_FUNCS: ContentTreeFuncs<TestRange> = {
  content_len_cur(val: TestRange): number {
    return val.exists && val.is_activated ? val.len : 0;
  },
  content_len_end(val: TestRange): number {
    return val.exists ? val.len : 0;
  },
  takes_up_space_cur(val: TestRange): boolean {
    return val.exists && val.is_activated;
  },
  takes_up_space_end(val: TestRange): boolean {
    return val.exists;
  },
  raw_len(val: TestRange): number {
    return val.len;
  },
  exists(val: TestRange): boolean {
    return val.exists;
  },
  none(): TestRange {
    return {
      id: Number.MAX_SAFE_INTEGER,
      len: Number.MAX_SAFE_INTEGER,
      is_activated: false,
      exists: false
    };
  },

  notify(val: TestRange, leaf: LeafIdx): void {
    // No-op for tests or implement logging here
    // console.log('notify!', val, leaf)
  },

  truncate(val: TestRange, at: number): TestRange {
    assert(val.exists)
    assert(at > 0 && at < val.len)

    const remainder: TestRange = {
      id: val.id + at,
      len: val.len - at,
      is_activated: val.is_activated,
      exists: val.exists
    }

    val.len = at

    return remainder;
  },

  truncate_keeping_right(val: TestRange, at: number): TestRange {
    const start = {
      ...val,
      len: at,
    }

    val.id += at
    val.len -= at

    return start
  },

  tryAppend(a: TestRange, b: TestRange): boolean {
    assert(a.exists && b.exists)

    if (a.id + a.len !== b.id) return false;
    if (a.is_activated !== b.is_activated) return false;

    a.len += b.len;
    return true;
  },

  find(val: TestRange, lv: LV): number {
    if (!val.exists) return -1;

    if (lv >= val.id && lv < val.id + val.len) {
      return lv - val.id;
    }

    return -1;
  }
}



function testSimpleInserts() {
  const tree = ctCreate(TEST_FUNCS)
  ct_debug_check(tree)

  let cursor = ct_cursor_at_start(tree)

  ct_insert(tree, {
    id: 123,
    len: 10,
    is_activated: false,
    exists: true
  }, cursor, true);

  cursor.offset = 2;
  ct_insert(tree, {
    id: 321,
    len: 20,
    is_activated: true,
    exists: true
  }, cursor, true);

  // console.log('cursor', cursor)
  ct_emplace_cursor(tree, 20, 2 + 20, cursor);
  ct_debug_check(tree);

  const items = [...ct_iter(tree)]
  const expected = [
    { id: 123, len: 2, is_activated: false, exists: true },
    { id: 321, len: 20, is_activated: true, exists: true },
    { id: 125, len: 8, is_activated: false, exists: true }
  ];
  assert.deepEqual(items, expected)
}


function testReplaceItem() {
  const tree = ctCreate(TEST_FUNCS)
  let cursor = ct_cursor_at_start(tree);

  ct_insert(tree, {
    id: 123,
    len: 10,
    is_activated: true,
    exists: true
  }, cursor, false);

  ct_emplace_cursor(tree, 10, 10, cursor);
  ct_debug_check(tree);

  const [end_pos, cursor2] = ct_cursor_before_cur_pos(tree, 2);
  console.assert(end_pos === 2, "End position should be 2");

  const [len, _] = ct_mutate_entry(tree, cursor2, 5, (e) => {
    console.assert(e.id === 125, "ID should be 125");
    console.assert(e.len === 5, "Length should be 5");
    e.is_activated = false;
    return null; // Return value not used in this example
  });

  console.assert(len === 5, "Length should be 5");
  ct_emplace_cursor(tree, 2, 7, cursor2);
  ct_debug_check(tree);

  // Verify the tree contents
  const items = [...ct_iter(tree)]
  const expected = [
    { id: 123, len: 2, is_activated: true, exists: true },
    { id: 125, len: 5, is_activated: false, exists: true },
    { id: 130, len: 3, is_activated: true, exists: true }
  ];

  assert.deepEqual(items, expected)
}


// Slow reference implementation for testing.
class RefContentTree<V> {
  private items: V[] = [];

  constructor(private funcs: ContentTreeFuncs<V>) {}

  /**
   * Find the index in the array after the given content position
   */
  idxAfterContentPos(dest_pos: number): number {
    let actual_pos = 0;
    let i = 0;
    while (actual_pos < dest_pos && i < this.items.length) {
      actual_pos += this.funcs.content_len_cur(this.items[i]);
      i += 1;
    }
    return i;
  }

  /**
   * Insert an item at the specified content position
   */
  insertContentPos(dest_pos: number, val: V) {
    assert(this.funcs.exists(val))
    assert(this.funcs.raw_len(val) >= 1)

    let i = this.idxAfterContentPos(dest_pos);

    // Split the value into chunks of size 1
    const chunks: V[] = [];

    while (this.funcs.raw_len(val) >= 2) {
      chunks.push(this.funcs.truncate_keeping_right(val, 1))
    }

    // chunks.push({...val})
    chunks.push(val)
    this.items.splice(i, 0, ...chunks);
  }

  /**
   * Mutate entries at the given content position
   */
  mutateEntriesBeforeContent(content_pos: number, n: number, mutate_fn: (val: V) => void) {
    let idx = this.idxAfterContentPos(content_pos);

    // Skip entries that don't take up space in current mode
    while (this.funcs.content_len_cur(this.items[idx]) === 0) idx += 1;

    // Mutate n entries. Note this is *weird* - surely we'd want to check if the items have
    // end state. But this matches the rust code so ???.
    for (let i = idx; i < idx + n && i < this.items.length; i++) {
      mutate_fn(this.items[i]);
    }
  }

  /**
   * Return an RLE-compressed iterator
   */
  *iter(): Generator<V> {
    if (this.items.length === 0) return;

    let current = { ...this.items[0] }

    for (let i = 1; i < this.items.length; i++) {
      const next = this.items[i];

      if (this.funcs.tryAppend(current, next)) {
        // Successfully merged
      } else {
        yield current;
        current = {...next}
      }
    }

    yield current;
  }

  /**
   * Get total length in current mode
   */
  get totalLenCur(): number {
    return this.items.reduce((sum, item) => sum + this.funcs.content_len_cur(item), 0);
  }

  /**
   * Get total length in end mode
   */
  get totalLenEnd(): number {
    // console.log('end', this.items.map(item => this.funcs.content_len_end(item)))
    return this.items.reduce((sum, item) => sum + this.funcs.content_len_end(item), 0);
  }
}


function fuzz(seed: number, verbose: boolean = false) {
  // Create a simple random number generator
  const random = SeedRandom(`zz ${seed}`)
  const randInt = (n: number) => Math.floor(random() * n)
  const randBool = (n: number = 0.5) => random() < n
  const randRange = (min: number, max: number) => (
    min >= max ? min : min + randInt(max - min)
  )

  // Fuzzing test setup
  const randomEntry = (): TestRange => {
    return {
      id: randInt(10),
      len: 1 + randInt(10),
      is_activated: randBool(0.5),
      exists: true
    };
  }

  const tree = ctCreate(TEST_FUNCS)
  const refTree = new RefContentTree<TestRange>(TEST_FUNCS);

  for (let i = 0; i < 1000; i++) {
    // if (verbose && i % 100 === 0) {
      console.log(`Iteration: ${i}`);
    // }

    if (i == 8) {
      debugger
    }

    // With 60% probability, insert a new item
    if (ct_total_cur_len(tree) === 0 || randBool(0.6)) {
      // Insert something
      const cur_pos = ct_total_cur_len(tree) === 0 ? 0 : randInt(ct_total_cur_len(tree) + 1);
      const item = randomEntry();

      if (verbose) {
        console.log(`Inserting ${JSON.stringify(item)} at position ${cur_pos}`);
      }

      // Insert into reference tree
      refTree.insertContentPos(cur_pos, {...item});

      // Insert into our tree
      let end_pos, cursor
      if (cur_pos === 0) {
        end_pos = 0
        cursor = ct_cursor_at_start(tree)
      } else {
        const [e, c] = ct_cursor_before_cur_pos(tree, cur_pos - 1);
        ct_inc_cursor_offset(tree, c);
        end_pos = e + 1
        cursor = c
      }

      const pre_pos = { cur: cur_pos, end: end_pos };
      ct_insert(tree, item, cursor, true)

      // Calculate expected positions after insertion
      const post_pos = {
        cur: pre_pos.cur + TEST_FUNCS.content_len_cur(item),
        end: pre_pos.end + TEST_FUNCS.content_len_end(item)
      };

      ct_emplace_cursor(tree, post_pos.cur, post_pos.end, cursor);

    } else {
      // Modify existing content
      const modify_len = 1 + randRange(1, Math.min(20, ct_total_cur_len(tree)));
      assert(modify_len <= ct_total_cur_len(tree))

      const pos = randInt(ct_total_cur_len(tree) - modify_len)
      const new_is_active = randBool(0.5);

      if (verbose) {
        console.log(`Modifying ${modify_len} items at position ${pos}, active=${new_is_active}`);
      }

      // Modify reference tree
      refTree.mutateEntriesBeforeContent(pos, modify_len, (e) => {
        e.is_activated = new_is_active;
      });

      // Modify our tree
      let len_remaining = modify_len;
      const [end_pos, cursor] = ct_cursor_before_cur_pos(tree, pos);
      let cursor_pos = { cur: pos, end: end_pos };

      while (len_remaining > 0) {
        const [changed, result] = ct_mutate_entry(tree, cursor, len_remaining, (e) => {
          e.is_activated = new_is_active;
          return {
            cur: TEST_FUNCS.content_len_cur(e),
            end: TEST_FUNCS.content_len_end(e)
          };
        });

        cursor_pos.cur += result.cur;
        cursor_pos.end += result.end;
        len_remaining -= changed;
      }

      console.log('emplace cursor', cursor, cursor_pos)
      ct_print_tree(tree)
      ct_emplace_cursor(tree, cursor_pos.cur, cursor_pos.end, cursor);
    }

    // Check tree invariants


    // Compare the two implementations
    // compareImplementations(tree, refTree, i, verbose);
    const ref = [...refTree.iter()]
    const actual = [...ct_iter_rle(tree)]

    try {
      ct_debug_check(tree);

      assert.equal(ct_total_cur_len(tree), refTree.totalLenCur)
      assert.equal(ct_total_end_len(tree), refTree.totalLenEnd)

      assert.deepEqual(ref, actual)
    } catch (e: any) {
      console.log('ref:')
      console.table(ref)
      console.log('tree:')
      console.table(actual)

      // console.log(tree)
      // console.log('leaves')

      // for (const leaf of ct_iter_leaves(tree)) {
      //   console.log('next', leaf.next)
      //   console.log('parent', leaf.parent)
      //   console.table(leaf.values)
      // }

      ct_print_tree(tree)

      throw e
    }
  }

  if (verbose) console.log(`Fuzzing completed successfully for seed ${seed}`);
}


// testSimpleInserts()
// testReplaceItem()

// fuzz(123, true)
fuzz(0, true)

/*

#[cfg(test)]
mod test {

    fn null_notify<V>(_v: V, _idx: LeafIdx) {}
    fn debug_notify<V: Debug>(v: V, idx: LeafIdx) {
        println!("Notify {:?} at {:?}", v, idx);
    }
    fn panic_notify<V>(_v: V, _idx: LeafIdx) {
        panic!("Notify erroneously called")
    }

    #[test]
    fn replace_item() {
        let mut tree: ContentTree<TestRange> = ContentTree::new();
        // let mut cursor = tree.cursor_at_start();
        let mut cursor = tree.mut_cursor_at_start();

        tree.insert_notify(TestRange {
            id: 123,
            len: 10,
            is_activated: true,
            exists: true,
        }, &mut cursor, &mut null_notify);
        tree.emplace_cursor((10, 10).into(), cursor);
        tree.dbg_check();

        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(2);
        assert_eq!(end_pos, 2);
        // assert_eq!(tree.get_cursor_pos(&cursor), LenPair::new(2, 2));
        // cursor.offset = 2;
        let (len, _r) = tree.mutate_entry(&mut cursor, 5, &mut panic_notify, |e| {
            assert_eq!(e.id, 125);
            assert_eq!(e.len, 5);
            e.is_activated = false;
        });
        assert_eq!(len, 5);
        tree.emplace_cursor((2, 7).into(), cursor);

        tree.dbg_check();

        // dbg!(tree.get_cursor_pos(&cursor));
        // dbg!(tree.iter().collect::<Vec<_>>());
        assert!(tree.iter().eq([
            TestRange { id: 123, len: 2, is_activated: true, exists: true },
            TestRange { id: 125, len: 5, is_activated: false, exists: true },
            TestRange { id: 130, len: 3, is_activated: true, exists: true },
        ].into_iter()));

        // Now re-activate part of the middle item.
        // let (end_pos, mut cursor) = tree.mut_cursor_at_end_pos(5);
        // I can't get a cursor where I want it. This is dirty as anything.

        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(1);
        assert_eq!(end_pos, 1);
        cursor.0.elem_idx += 1; cursor.0.offset = 3; // hack hack hack.
        let (len, _r) = tree.mutate_entry(&mut cursor, 5, &mut panic_notify, |e| {
            // dbg!(&e);
            e.is_activated = true;
        });
        assert!(tree.iter().eq([
            TestRange { id: 123, len: 2, is_activated: true, exists: true },
            TestRange { id: 125, len: 3, is_activated: false, exists: true },
            TestRange { id: 128, len: 5, is_activated: true, exists: true },
        ].into_iter()));
        assert_eq!(len, 2);
        // dbg!(tree.iter().collect::<Vec<_>>());

        tree.emplace_cursor((4, 7).into(), cursor);
        tree.dbg_check();
    }


//     use std::ops::Range;
//     use std::pin::Pin;
//     use rand::prelude::SmallRng;
//     use rand::{Rng, SeedableRng, thread_rng};
//     use content_tree::{ContentTreeRaw, null_notify, RawPositionMetricsUsize};
//     use crate::list_fuzzer_tools::fuzz_multithreaded;
//     use super::*;
//
//     #[derive(Debug, Copy, Clone, Eq, PartialEq)]
//     enum Foo { A, B, C }
//     use Foo::*;
//
//     #[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
//     struct X(usize);
//     impl IndexContent for X {
//         fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
//             debug_assert!(offset > 0);
//             debug_assert!(other_len > 0);
//             &self.at_offset(offset) == other
//         }
//
//         fn at_offset(&self, offset: usize) -> Self {
//             X(self.0 + offset)
//         }
//
//         fn eq(&self, other: &Self, _upto_len: usize) -> bool {
//             self.0 == other.0
//         }
//     }
//
//     #[test]
//     fn empty_tree_is_empty() {
//         let tree = ContentTree::<X>::new();
//
//         tree.dbg_check_eq(&[]);
//     }
//
//     #[test]
//     fn overlapping_sets() {
//         let mut tree = ContentTree::new();
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
//         // assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);
//         // dbg!(&tree.leaves[0]);
//         tree.set_range((5..11).into(), X(200));
//         tree.dbg_check_eq(&[RleDRun::new(5..11, X(200))]);
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[
//             RleDRun::new(5..10, X(100)),
//             RleDRun::new(10..11, X(205)),
//         ]);
//
//         tree.set_range((2..50).into(), X(300));
//         // dbg!(&tree.leaves);
//         tree.dbg_check_eq(&[RleDRun::new(2..50, X(300))]);
//
//     }
//
//     #[test]
//     fn split_values() {
//         let mut tree = ContentTree::new();
//         tree.set_range((10..20).into(), X(100));
//         tree.set_range((12..15).into(), X(200));
//         tree.dbg_check_eq(&[
//             RleDRun::new(10..12, X(100)),
//             RleDRun::new(12..15, X(200)),
//             RleDRun::new(15..20, X(105)),
//         ]);
//     }
//
//     #[test]
//     fn set_inserts_1() {
//         let mut tree = ContentTree::new();
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
//
//         tree.set_range((5..10).into(), X(200));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(200))]);
//
//         // dbg!(&tree);
//         tree.set_range((15..20).into(), X(300));
//         // dbg!(tree.iter().collect::<Vec<_>>());
//         tree.dbg_check_eq(&[
//             RleDRun::new(5..10, X(200)),
//             RleDRun::new(15..20, X(300)),
//         ]);
//
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//     }
//
//     #[test]
//     fn set_inserts_2() {
//         let mut tree = ContentTree::new();
//         tree.set_range((5..10).into(), X(100));
//         tree.set_range((1..5).into(), X(200));
//         // dbg!(&tree);
//         tree.dbg_check_eq(&[
//             RleDRun::new(1..5, X(200)),
//             RleDRun::new(5..10, X(100)),
//         ]);
//         dbg!(&tree.leaves[0]);
//
//         tree.set_range((3..8).into(), X(300));
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//         tree.dbg_check_eq(&[
//             RleDRun::new(1..3, X(200)),
//             RleDRun::new(3..8, X(300)),
//             RleDRun::new(8..10, X(103)),
//         ]);
//     }
//
//     #[test]
//     fn split_leaf() {
//         let mut tree = ContentTree::new();
//         // Using 10, 20, ... so they don't merge.
//         tree.set_range(10.into(), X(100));
//         tree.dbg_check();
//         tree.set_range(20.into(), X(200));
//         tree.set_range(30.into(), X(100));
//         tree.set_range(40.into(), X(200));
//         tree.dbg_check();
//         // dbg!(&tree);
//         tree.set_range(50.into(), X(100));
//         tree.dbg_check();
//
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//
//         tree.dbg_check_eq(&[
//             RleDRun::new(10..11, X(100)),
//             RleDRun::new(20..21, X(200)),
//             RleDRun::new(30..31, X(100)),
//             RleDRun::new(40..41, X(200)),
//             RleDRun::new(50..51, X(100)),
//         ]);
//     }
//

    impl ContentLength for TestRange {
        fn content_len(&self) -> usize { self.content_len_cur() }

        fn content_len_at_offset(&self, offset: usize) -> usize {
            if self.is_activated { offset } else { 0 }
        }
    }

    fn random_entry(rng: &mut SmallRng) -> TestRange {
        TestRange {
            id: rng.gen_range(0..10),
            len: rng.gen_range(1..10),
            is_activated: rng.gen_bool(0.5),
            exists: true,
        }
    }

    fn fuzz(seed: u64, mut verbose: bool) {
        verbose = verbose; // suppress mut warning.
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree = ContentTree::<TestRange>::new();
        // let mut check_tree: Pin<Box<ContentTreeRaw<RleDRun<Option<i32>>, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        let mut check_tree: Pin<Box<ContentTreeRaw<TestRange, FullMetricsUsize>>> = ContentTreeRaw::new();
        const START_JUNK: u32 = 1_000_000;
        check_tree.replace_range_at_offset(0, TestRange {
            id: START_JUNK,
            len: START_JUNK,
            is_activated: false,
            exists: false,
        });

        for _i in 0..1000 {
            if verbose { println!("i: {}", _i); }
            // println!("i: {}", _i);

            // if _i == 31 {
            //     println!("asdf");
            //     // verbose = true;
            // }

            if tree.total_len().cur == 0 || rng.gen_bool(0.6) {

                // tree.dbg_check();
                // Insert something.
                let cur_pos = rng.gen_range(0..=tree.total_len().cur);
                let item = random_entry(&mut rng);

                if verbose { println!("inserting {:?} at {}", item, cur_pos); }

                // Insert into check tree
                {
                    // check_tree.check();
                    // check_tree.print_ptr_tree();
                    let mut cursor = check_tree.mut_cursor_at_content_pos(cur_pos, true);
                    cursor.insert(item);
                    assert_eq!(cursor.count_content_pos(), cur_pos + item.content_len_cur());
                }

                // Insert into our tree.
                {
                    // if verbose { dbg!(&tree); }

                    // This code mirrors the equivalent code in merge.rs
                    let (end_pos, mut cursor) = if cur_pos == 0 {
                        (0, tree.mut_cursor_at_start())
                    } else {
                        // // Equivalent of getting a cursor with stick_end: true.
                        // let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        // tree.emplace_cursor((cur_pos - 1, end_pos).into(), cursor);
                        //
                        // let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        // tree.cursor_inc_offset(&mut cursor);
                        // tree.emplace_cursor((cur_pos, end_pos + 1).into(), cursor);


                        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        cursor.0.inc_offset(&tree);
                        (end_pos + 1, cursor)
                    };
                    // let mut cursor = tree.cursor_at_content_pos::<false>(pos);
                    // dbg!(&cursor);
                    let pre_pos = LenPair::new(cur_pos, end_pos);
                    tree.insert_notify(item, &mut cursor, &mut null_notify);
                    // dbg!(&cursor);

                    // if verbose { dbg!(&tree); }
                    // tree.dbg_check();

                    // This will check that the position makes sense.
                    tree.emplace_cursor(pre_pos + item.content_len_pair(), cursor);

                    // let post_pos = tree.get_cursor_pos(&cursor);
                    // // dbg!(pre_pos, item.content_len_pair(), post_pos);
                    // assert_eq!(pre_pos + item.content_len_pair(), post_pos);
                }
            } else {

                let gen_range = |rng: &mut SmallRng, range: Range<usize>| {
                    if range.is_empty() { range.start }
                    else { rng.gen_range(range) }
                };

                // Modify something.
                //
                // Note this has a subtle sort-of flaw: The first item we touch will always be
                // active. But we might make some later items active again in the range.
                let modify_len = gen_range(&mut rng, 1..20.min(tree.total_len().cur));
                // let modify_len = 1;
                debug_assert!(modify_len <= tree.total_len().cur);
                let pos = gen_range(&mut rng, 0..tree.total_len().cur - modify_len);
                let new_is_active = rng.gen_bool(0.5);

                // The chunking of the two tree implementations might differ, so we'll run modify
                // in a loop.
                {
                    let mut len_remaining = modify_len;
                    let mut cursor = check_tree.mut_cursor_at_content_pos(pos, false);
                    while len_remaining > 0 {
                        let (changed, _) = cursor.mutate_single_entry_notify(len_remaining, content_tree::null_notify, |e| {
                            e.is_activated = new_is_active;
                        });
                        cursor.roll_to_next_entry();
                        len_remaining -= changed;
                    }
                }

                {
                    let mut len_remaining = modify_len;
                    // let mut cursor = tree.cursor_at_content_pos::<false>(pos);
                    let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(pos);
                    let mut cursor_pos = LenPair::new(pos, end_pos);

                    while len_remaining > 0 {
                        // let pre_pos = tree.get_cursor_pos(&cursor);
                        let (changed, len_here) = tree.mutate_entry(&mut cursor, len_remaining, &mut null_notify, |e| {
                            e.is_activated = new_is_active;
                            e.content_len_pair()
                        });
                        cursor_pos += len_here;
                        // let post_pos = tree.get_cursor_pos(&cursor);
                        // assert_eq!(pre_pos.end + changed, post_pos.end);
                        len_remaining -= changed;
                    }

                    tree.emplace_cursor(cursor_pos, cursor);
                }
            }

            // Check that both trees have identical content.
            tree.dbg_check();
            assert!(check_tree.iter().filter(|e| e.id < START_JUNK)
                .eq(tree.iter_rle()));
        }
    }

    #[test]
    fn content_tree_fuzz_once() {
        // fuzz(3322, true);
        // for seed in 8646911284551352000..8646911284551353000 {
        //
        //     fuzz(seed, true);
        // }
        fuzz(0, true);
    }

    #[test]
    #[ignore]
    fn content_tree_fuzz_forever() {
        fuzz_multithreaded(u64::MAX, |seed| {
            if seed % 100 == 0 {
                println!("Iteration {}", seed);
            }
            fuzz(seed, false);
        })
    }
}






*/

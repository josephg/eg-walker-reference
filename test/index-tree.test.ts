import { test, describe } from "bun:test";
import assert from "assert/strict";
import { itClear, itCountItems, itCreate, itDbgCheck, itGetEntry, itSetRange, ITContent, MAX_BOUND, RleDRun } from "../src/index-tree.js";
import SeedRandom from "seed-random";

type SimpleContent = number | null
const simpleFuncs: ITContent<SimpleContent> = {
  at_offset(val, offset) {
    return val == null ? null : val + offset
  },

  try_append(val, offset, other, other_len) {
    assert(offset > 0)
    assert(other_len > 0)

    // This would be much nicer with a rust match statement.
    if ((val == null) !== (other == null)) return false // Null cannot combine with anything else.
    if (val == null && other == null) return true // Nulls can combine.

    // Otherwise add val and offset together.
    return val! + offset == other
  },

  // eq(val, other, upto_len) {
  //   return val == other
  // },

  default() { return null },
}


// describe('index-tree', () => {
//   test("create and clear", () => {
//     const tree = it_create(simpleFuncs);
//     // An empty tree contains the default value.
//     assert.deepEqual([...tree], [{start: 0, end: MAX_BOUND, val: null}])

//     it_set_range(tree, 0, 10, 1);
//     assert.deepEqual([...tree], [
//       {start: 0, end: 10, val: 1},
//       {start: 10, end: MAX_BOUND, val: null},
//     ])

//     it_clear(tree)
//     assert.deepEqual([...tree], [{start: 0, end: MAX_BOUND, val: null}])
//   })


//   test("set and get entries", () => {
//     const tree = it_create(simpleFuncs);

//     it_set_range(tree, 0, 10, 100);
//     it_set_range(tree, 10, 20, 200);

//     let entry = it_get_entry(tree, 5);
//     assert.deepEqual(entry, { start: 0, end: 10, val: 100 });

//     entry = it_get_entry(tree, 15);
//     assert.deepEqual(entry, { start: 10, end: 20, val: 200 });

//     // Overwrite part of an existing range
//     it_set_range(tree, 5, 15, 300);

//     entry = it_get_entry(tree, 0);
//     assert.deepEqual(entry, { start: 0, end: 5, val: 100 });

//     entry = it_get_entry(tree, 7);
//     assert.deepEqual(entry, { start: 5, end: 15, val: 300 });

//     entry = it_get_entry(tree, 17);
//     assert.deepEqual(entry, { start: 15, end: 20, val: 205 }); // Note the val is 205 now.

//     // Check the default value beyond the set ranges
//     entry = it_get_entry(tree, 25);
//     assert.deepEqual(entry, { start: 20, end: MAX_BOUND, val: null });
//   });

//   test("iterator", () => {
//     const tree = it_create(simpleFuncs);

//     it_set_range(tree, 0, 10, 1);
//     it_set_range(tree, 10, 20, 2);
//     it_set_range(tree, 20, 30, 3);

//     const entries = [...tree];
//     assert.equal(entries.length, 4);
//     assert.deepEqual(entries, [
//       { start: 0, end: 10, val: 1 },
//       { start: 10, end: 20, val: 2 },
//       { start: 20, end: 30, val: 3 },
//       { start: 30, end: MAX_BOUND, val: null },
//     ]);
//   });

//   test("count items", () => {
//     const tree = it_create(simpleFuncs);

//     assert.equal(it_count_items(tree), 1); // Default range counts as one item

//     it_set_range(tree, 0, 10, 1);
//     it_set_range(tree, 10, 20, 2);

//     assert.equal(it_count_items(tree), 3); // Two set ranges plus the default range
//   });

//   test("complex operations", () => {
//     const tree = it_create(simpleFuncs);

//     // Set initial ranges
//     it_set_range(tree, 0, 100, 1000);
//     it_set_range(tree, 100, 200, 2000);

//     // Overwrite with smaller ranges
//     it_set_range(tree, 50, 75, 3000);
//     it_set_range(tree, 150, 175, 4000);

//     // Check the resulting structure
//     assert.deepEqual([...tree], [
//       { start: 0, end: 50, val: 1000 },
//       { start: 50, end: 75, val: 3000 },
//       { start: 75, end: 100, val: 1075 },
//       { start: 100, end: 150, val: 2000 },
//       { start: 150, end: 175, val: 4000 },
//       { start: 175, end: 200, val: 2075 },
//       { start: 200, end: MAX_BOUND, val: null },
//     ]);

//     // Overwrite with a range that spans multiple existing ranges
//     it_set_range(tree, 25, 125, 5000);

//     assert.deepEqual([...tree], [
//       { start: 0, end: 25, val: 1000 },
//       { start: 25, end: 125, val: 5000 },
//       { start: 125, end: 150, val: 2025 },
//       { start: 150, end: 175, val: 4000 },
//       { start: 175, end: 200, val: 2075 },
//       { start: 200, end: MAX_BOUND, val: null },
//     ]);
//   });
// });

// For fuzz testing, we'll compare the index-tree to a simple parallel reference implementation
// built just using a list.

import assert from "assert/strict";
import { itClear, itCountItems, itCreate, itDbgCheck, itGetEntry, itSetRange, ITContent, MAX_BOUND, RleDRun } from "../src/index-tree.js";
import SeedRandom from "seed-random";

type SimpleContent = number | null
const simpleFuncs: ITContent<SimpleContent> = {
  atOffset(val, offset) {
    return val == null ? null : val + offset
  },

  tryAppend(val, offset, other, other_len) {
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


interface SimpleContainer<V> {
  list: V[],
  funcs: ITContent<V>,
  [Symbol.iterator](): Iterator<RleDRun<V>>,
}

function makeSimpleContainer<V>(funcs: ITContent<V>): SimpleContainer<V> {
  return {
    list: [],
    funcs,
    // clear() {
    //   this.list.length = 0
    // },
    *[Symbol.iterator]() {
      let start = 0;
      if (this.list.length > 0) {
        let currentVal = this.list[0];

        for (let i = 1; i < this.list.length; i++) {
          if (!this.funcs.tryAppend(currentVal, i - start, this.list[i], 1)) {
            yield {start, end: i, val: currentVal};
            start = i;
            currentVal = this.list[i];
          }
        }

        // Yield the last entry
        yield {start, end: this.list.length, val: currentVal};
        start = this.list.length
      }

      yield {start, end: MAX_BOUND, val: funcs.default()}
    }
  }
}

function sc_clear<V>(c: SimpleContainer<V>) {
  c.list.length = 0
}

function sc_set_range(c: SimpleContainer<SimpleContent>, start: number, end: number, value: SimpleContent) {
  for (let i = c.list.length; i < start; i++) {
    c.list.push(c.funcs.default())
  }
  for (let i = start; i < end; i++) {
    c.list[i] = value == null ? value : value + (i - start)
  }
}

function assertIterEq<V>(a: Iterable<V>, b: Iterable<V>) {
  let ia = a[Symbol.iterator]()
  let ib = b[Symbol.iterator]()

  while (true) {
    let aa = ia.next()
    let bb = ib.next()
    assert.equal(aa.done, bb.done)
    if (aa.done) break

    assert.deepEqual(aa.value, bb.value)
  }
}

function fuzz(seed: string, verbose: boolean = false) {
  const random = SeedRandom(`zz ${seed}`);
  const randInt = (n: number) => Math.floor(random() * n);

  const tree = itCreate(simpleFuncs)
  const checkTree = makeSimpleContainer(simpleFuncs)

  for (let i = 0; i < 1000; i++) {
    if (verbose) console.log(`i: ${i}`);

    // Generate some overlapping ranges sometimes but not too many
    const val = randInt(100) + 100;
    const start = randInt(100);
    const len = randInt(100) + 1;

    const end = start + len

    // if (i == 28) {
    //   debugger
    //   console.log('Tree:', [...tree])
    //   console.log('start:', start, 'end', end, 'val', val)
    //   it_dbg_check(tree)
    // }
    itSetRange(tree, start, end, val)
    itDbgCheck(tree)
    sc_set_range(checkTree, start, end, val);

    // console.log('TREE', [...tree])
    // console.log('CHEK', [...checkTree])

    // assert.deepStrictEqual
    assertIterEq(tree, checkTree)
  }

  // console.log("Fuzzing completed successfully!");
}

// debugger
// Example usage
// fuzz("sadas", true);

for (let i = 0; i < 100000000; i++) {
  if (i % 100 === 0) console.log('fuzz', i)
  fuzz(`zz ${i}`, false)
}

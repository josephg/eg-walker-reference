import { ITContent, MAX_BOUND } from "./index-tree.js";
import { LeafIdx, LV } from "./tree-common.js";

export type Marker = {
  type: 'ins',
  leaf: LeafIdx // The content tree leaf which contains this marker item.
} | {
  // Delete markers describe the target of delete operations. The target is always
  // an inserted item.
  type: 'del',
  target: LV,
  fwd: boolean, // Is this "forwards" (as in, 1,2,3). If false, we're backwards (3,2,1).
}

const DEFAULT_MARKER: Marker = Object.freeze<Marker>({
  type: "ins",
  leaf: MAX_BOUND,
})

export const MARKER_FUNCS: ITContent<Marker> = {
  tryAppend(a, offset, b, other_len): boolean {
    if (a.type === 'ins' && b.type === 'ins') {
      return a.leaf === b.leaf
    } else if (a.type === 'del' && b.type === 'del') {
      // This is quite complex, but it should be correct. This code was lifted
      // more or less directly from diamond types.

      // Can we append forwards? Items default to forwards so this is trivial.
      if (a.fwd && b.fwd && a.target + offset == b.target) return true

      // Can we append backwards? This is horrible. First figure out
      // the expected resulting a.target value. If we can append backward, they will
      // match.
      let a_start = !a.fwd
        ? a.target
        : offset === 1
          ? a.target + 1
          : -1 // None.

      let b_start = !b.fwd
        ? b.target + offset
        : other_len === 1
          ? b.target + 1 + offset
          : -1 // None.

      if (a_start >= 0 && b_start >= 0 && a_start === b_start) {
        // Yay!
        a.target = b_start
        a.fwd = false
        return true
      }
    }
    return false
  },

  atOffset(val, offset) {
    if (val.type === 'ins') return val
    else return {
      type: 'del',
      target: val.target + (val.fwd ? offset : -offset),
      fwd: val.fwd
    }
  },

  default() {
    return DEFAULT_MARKER
  },
}

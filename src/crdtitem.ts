import { ContentTreeFuncs } from "./content-tree.js"
import { LV } from "./tree-common.js"

// This is a bit gross, but we need some region of placeholder LVs.
export const PLACEHOLDER_START: LV = Math.floor(Number.MAX_SAFE_INTEGER / 4)

export enum ItemState {
  NotYetInserted = -1,
  Inserted = 0,
  Deleted = 1, // Or some +ive number of times the item has been deleted.
}

// This is internal only, and used while reconstructing the changes.
export interface CRDTItem {
  // This item represents a span of inserted characters from lvStart to lvEnd.
  lvStart: LV,
  lvEnd: LV,

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
  endStateEverDeleted: boolean, // Replace this with a boolean?

  // -1 means start / end of document. This is the core list CRDT (sync9/fugue).
  originLeft: LV | -1,

  // -1 means the end of the document. This uses fugue's semantics.
  // All right children have a rightParent of -1.
  originRight: LV | -1,
}

// A placeholder item corresponds to the set of items which existed in the
// document before we started merging changes. See the paper for details.
export const createPlaceholderItem = (): CRDTItem => ({
  lvStart: PLACEHOLDER_START,
  lvEnd: PLACEHOLDER_START * 2 - 1,
  curState: ItemState.Inserted,
  endStateEverDeleted: false,
  originLeft: -1,
  originRight: -1,
})

export const itemTakesUpCurSpace = (e: CRDTItem) => e.curState === ItemState.Inserted
export const itemTakesUpEndSpace = (e: CRDTItem) => !e.endStateEverDeleted
export const itemLen = (e: CRDTItem) => e.lvEnd - e.lvStart

export const ITEM_FUNCS: ContentTreeFuncs<CRDTItem> = {
  takes_up_space_cur: itemTakesUpCurSpace,
  takes_up_space_end: itemTakesUpEndSpace,
  content_len_cur(val) {
    return val.curState === ItemState.Inserted
      ? val.lvEnd - val.lvStart
      : 0
  },
  content_len_end(val) {
    return !val.endStateEverDeleted
      ? val.lvEnd - val.lvStart
      : 0
  },
  raw_len: itemLen,

  truncate(val, offset): CRDTItem {
    let result: CRDTItem = {
      lvStart: val.lvStart + offset,
      lvEnd: val.lvEnd,
      originLeft: val.lvStart + offset - 1,
      originRight: val.originRight,
      curState: val.curState,
      endStateEverDeleted: val.endStateEverDeleted,
    }

    val.lvEnd = val.lvStart + offset
    return result
  },

  truncate_keeping_right(val, offset): CRDTItem {
    let result: CRDTItem = {
      lvStart: val.lvStart,
      lvEnd: val.lvStart + offset,
      originLeft: val.originLeft,
      originRight: val.originRight,
      curState: val.curState,
      endStateEverDeleted: val.endStateEverDeleted,
    }

    val.originLeft = val.lvStart + offset - 1
    val.lvStart += offset
    return result
  },

  tryAppend(a, b): boolean {
    if (a.lvEnd === b.lvStart
      && b.originLeft === b.lvStart - 1
      && a.originRight === b.originRight
      && a.curState === b.curState
      && a.endStateEverDeleted === b.endStateEverDeleted)
    {
      // Append.
      a.lvEnd = b.lvEnd
      return true
    } else return false
  },

  find(val, lv) {
   return (lv >= val.lvStart && lv < val.lvEnd)
    ? lv - val.lvStart
    : -1
  },
}

import bs from 'binary-search'
// import { assert } from '../utils.js'
import assert from 'node:assert/strict'

/**
 * An RleList is a list of items which automatically merges
 * adjacent elements whenever possible.
 */
export interface RleList<T> {
  list: T[],
  methods: MergeAndSplitMethods<T>,
}

export interface MergeAndSplitMethods<T> {
  // canAppend(into: T, from: T): boolean,
  // append(into: T, from: T): void,
  tryAppend(into: T, from: T): boolean,

  len(item: T): number,

  /**
   * Offset must be between 1 and item len -1. The item is truncated
   * in-place and the remainder is returned.
   */
  truncate?(item: T, offset: number): T,
  // split?(item: T, offset: number): [T, T],

  getKey?(item: T): number,

  cloneItem(item: T): T,
}


export function rlePush<T>(list: T[], m: MergeAndSplitMethods<T>, newItem: T) {
  if (list.length === 0 || !m.tryAppend(list[list.length - 1], newItem)) {
    list.push(newItem)
  }
}

/** Insert the new item in its corresponding location in the list */
export function rleInsert<T>(list: T[], m: MergeAndSplitMethods<T>, newItem: T) {
  const {getKey} = m
  if (getKey == null) throw Error('Cannot insert with no getKey method')

  const newKey = getKey(newItem)
  if (list.length === 0 || newKey >= getKey(list[list.length - 1])) {
    // Just push the new entry to the end of the list like normal.
    rlePush(list, m, newItem)
  } else {
    // We need to splice the new entry in. Find the index of the previous entry...
    let idx = bs(list, newKey, (entry, needle) => getKey(entry) - needle)
    if (idx >= 0) throw Error('Invalid state - item already exists')

    idx = - idx - 1 // The destination index is the 2s compliment of the returned index.

    // Try to append it to the previous item.
    if (idx > 0 && m.tryAppend(list[idx - 1], newItem)) return // yey

    // That didn't work! Try to prepend it to the next item.
    if (idx < list.length - 1 && m.tryAppend(newItem, list[idx])) {
      // We've modified newItem to include list[idx].
      list[idx] = newItem
      return
    }

    // Failing all that, just splice it in.
    list.splice(idx, 0, newItem)
  }
}

/** Iterate (and yield) the items in the half-open range from [startKey..endKey) */
export function *rleIterRange<T>(list: T[], m: MergeAndSplitMethods<T>, startKey: number, endKey: number) {
  if (startKey === endKey) return

  const {getKey, truncate} = m
  if (getKey == null || truncate == null) throw Error('Cannot insert with no getKey method')

  let idx = bs(list, startKey, (entry, needle) => {
    const key = getKey(entry)
    return (needle >= key && needle < key + m.len(entry))
      ? 0 // Return 0 if needle is actually within the item.
      : key - needle
  })
  if (idx < 0) idx = -idx - 1

  for (; idx < list.length; idx++) {
    let item = list[idx]
    const key = getKey(item)
    if (key >= endKey) break

    const len = m.len(item)

    // Just a check - the item must overlap with the start-end range.
    assert(key + len > startKey)

    if (key < startKey) {
      // Trim the item.
      item = truncate(m.cloneItem(item), startKey - key)
    }

    if (key + len > endKey) {
      item = m.cloneItem(item) // Might double-clone. Eh.
      truncate(item, endKey - key)
    }

    yield item
  }
}

{
  type TT = {key: number, len: number, val: string}
  const methods: MergeAndSplitMethods<TT> = {
    cloneItem(i) { return {...i} },
    len(i) { return i.len },
    tryAppend(a, b) {
      if (a.key + a.len === b.key && a.val === b.val) {
        a.len += b.len
        return true
      } else return false
    },
    getKey(i) { return i.key },
    truncate(i, offset) {
      const other: TT = {
        key: i.key + offset,
        len: i.len - offset,
        val: i.val,
      }
      i.len = offset
      return other
    },
  }

  {
    const list: TT[] = []
    rlePush(list, methods, {key: 10, len: 2, val: 'a'})
    rlePush(list, methods, {key: 12, len: 2, val: 'a'})
    assert(list.length === 1)
    assert(list[0].len === 4)
  }

  {
    const list: TT[] = []

    const empty = [...rleIterRange(list, methods, 11, 13)]
    assert(empty.length === 0)

    rlePush(list, methods, {key: 10, len: 2, val: 'a'})
    rlePush(list, methods, {key: 12, len: 2, val: 'b'})
    const iterResult1 = [...rleIterRange(list, methods, 11, 13)]
    assert.deepEqual(iterResult1, [
      {key: 11, len: 1, val: 'a'},
      {key: 12, len: 1, val: 'b'}
    ])

    assert.deepEqual([...rleIterRange(list, methods, 5, 12)], [
      {key: 10, len: 2, val: 'a'}
    ])
    assert.deepEqual([...rleIterRange(list, methods, 10, 12)], [
      {key: 10, len: 2, val: 'a'}
    ])
    assert.deepEqual([...rleIterRange(list, methods, 0, 10000)], [
      {key: 10, len: 2, val: 'a'},
      {key: 12, len: 2, val: 'b'}
    ])
  }
}

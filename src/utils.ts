// ** A couple utility methods **
export function assert(expr: boolean, msg?: string) {
  if (!expr) {
    const err = Error(msg != null ? `Assertion failed: ${msg}` : 'Assertion failed')
    Error.captureStackTrace(err, assert)
    throw err
  }
}

export function assertEq<T>(a: unknown, b: T, msg?: string): asserts a is T {
  if (a !== b) {
    const err = Error(`Assertion failed: ${a} !== ${b} ${msg ?? ''}`)
    Error.captureStackTrace(err, assertEq)
    throw err
  }
}

export function assertNe<T>(a: unknown, b: unknown, msg?: string) {
  if (a === b) {
    const err = Error(`Assertion failed: ${a} === ${b} ${msg ?? ''}`)
    Error.captureStackTrace(err, assertNe)
    throw err
  }
}

/**
 * Try to push an item into the list. Takes a "tryAppend" function which optimistically
 * tries to merge the item with the previous item, and returns true if successful.
 */
export const pushRLEList = <T>(tryAppend: (a: T, b: T) => boolean, list: T[], newItem: T) => {
  if (list.length === 0 || !tryAppend(list[list.length - 1], newItem)) {
    list.push(newItem)
  }
}

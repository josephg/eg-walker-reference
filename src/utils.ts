// ** A couple utility methods **
export function assert(expr: boolean, msg?: string) {
  if (!expr) throw Error(msg != null ? `Assertion failed: ${msg}` : 'Assertion failed')
}

export function assertEq<T>(a: unknown, b: T, msg?: string): asserts a is T {
  if (a !== b) throw Error(`Assertion failed: ${a} !== ${b} ${msg ?? ''}`)
}

export function assertNe<T>(a: unknown, b: unknown, msg?: string) {
  if (a === b) throw Error(`Assertion failed: ${a} === ${b} ${msg ?? ''}`)
}

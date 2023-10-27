import { RawVersion } from "./types.js"

export type AgentVersion = RawVersion
export const createRandomId = (): string => Math.random().toString(36).slice(2)
// export function createAgent(): AgentVersion {
//   const agent = Math.random().toString(36).slice(2)
//   return [agent, 0]
// }
export const nextVersion = (agent: AgentVersion): AgentVersion => {
  return [agent[0], agent[1]++]
}

// export function createAgent(): Agent {
//   const agent = Math.random().toString(36).slice(2)
//   let seq = 0
//   return () => ([agent, seq++])
// }

export const assertSortedCustom = <T>(v: T[], f: (t: T) => number) => {
  for (let i = 1; i < v.length; i++) {
    if (f(v[i-1]) >= f(v[i])) throw Error('Version not sorted')
  }
}

export const assertSorted = (v: number[]) => {
  for (let i = 1; i < v.length; i++) {
    if (v[i-1] >= v[i]) throw Error('Version not sorted')
  }
}

export const errExpr = (str: string): never => { throw Error(str) }

export function assert(expr: boolean, msg?: string) {
  if (!expr) throw Error(msg != null ? `Assertion failed: ${msg}` : 'Assertion failed')
}

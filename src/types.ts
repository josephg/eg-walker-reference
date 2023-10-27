// import type * as causalGraph from "./causal-graph.js"

export type RawVersion = [agent: string, seq: number]

export const ROOT: RawVersion = ['ROOT', 0]

/** Local version */
export type LV = number

/** Local version range. Range is [start, end). */
export type LVRange = [start: number, end: number]

export const ROOT_LV: LV = -1

// export type VersionSummary = [string, [number, number][]][]
export interface VersionSummary {[agent: string]: [number, number][]}
// export type RawHeads = RawVersion[]

export type Pair<T> = [LV, T]

// This is a helper library for storing & interacting with a run-length encoded causal graph
// (join semilattice) of changes.
//
// All changes can be referenced as either a [agent, seq] pair or as a "local version" (essentially
// a local, autoincremented ID per known version).
//
// The causal graph only stores a set of known versions and each version's parent version.
// The operations themselves are not stored here.
//
// The versions are is stored in runs, and run-length encoded. Compression depends
// on concurrency. (High concurrency = bad compression. Low concurrency = great compression).

import PriorityQueue from 'priorityqueuejs'
import bs from 'binary-search'
import { assert, max2, min2, pushRLEList } from './utils.js'

export interface VersionSummary {[agent: string]: [number, number][]}

export type Id = [agent: string, seq: number]

/** Local version */
export type LV = number

/** Local version range. Range is [start, end). */
export type LVRange = [start: number, end: number]



export class CausalGraph {
  inner: CausalGraphInner

  constructor() {
    this.inner = createCG()
  }

  nextSeqForAgent(agent: string): number {
    return nextSeqForAgent(this.inner, agent)
  }

  add(agent: string, seqStart: number, seqEnd: number, parents?: LV[]): number {
    return add(this.inner, agent, seqStart, seqEnd, parents)
  }

  addRemote(id: Id, len: number = 1, rawParents?: Id[]): number {
    return addRemote(this.inner, id, len, rawParents)
  }

  dbgCheck() {
    checkCG(this.inner)
  }

  lvToId(lv: LV): Id {
    return lvToId(this.inner, lv)
  }

  lvToIdList(frontier: LV[]) {
    return lvToIdList(this.inner, frontier)
  }

  idToLV(agent: string, seq: number): LV {
    return idToLV(this.inner, agent, seq)
  }

  idToLVList(ids: Id[]): LV[] {
    return idToLVList(this.inner, ids)
  }

  heads() {
    return this.inner.heads
  }

  summarizeVersion() {
    return summarizeVersion(this.inner)
  }

  serializeDiff(ranges: LVRange[]) {
    return serializeDiff(this.inner, ranges)
  }

  mergePartialVersions(cgDiff: PartialSerializedCG) {
    return mergePartialVersions(this.inner, cgDiff)
  }


  diff(a: LV[], b: LV[]) {
    return diff(this.inner, a, b)
  }

  findConflicting(a: LV[], b: LV[], visit: (range: LVRange, flag: DiffFlag) => void): LV[] {
    return findConflicting(this.inner, a, b, visit)
  }

  nextLV() {
    return nextLV(this.inner)
  }

  iterVersionsBetween(vStart: LV, vEnd: LV): Generator<CGEntry> {
    return iterVersionsBetween(this.inner, vStart, vEnd)
  }

  findDominators(versions: LV[]): LV[] {
    return findDominators(this.inner, versions)
  }
}



export type CGEntry = {
  version: LV,
  vEnd: LV, // > version.

  agent: string,
  seq: number, // Seq for version.

  parents: LV[] // Parents for version
}

type ClientEntry = {
  seq: number,
  seqEnd: number,
  /** LV of the first item in this run */
  version: LV,
}

export interface CausalGraphInner {
  /** Current global version frontier */
  heads: LV[],

  /** Map from localversion -> rawversion */
  entries: CGEntry[],

  /** Map from agent -> list of versions by that agent. Entries sorted by seq. */
  agentToVersion: {[k: string]: ClientEntry[]},
}

export const createCG = (): CausalGraphInner => ({
  heads: [],
  entries: [],
  agentToVersion: {},
})

// // This is a variant of pushRLEList when we aren't sure if the new item will actually
// // be appended to the end of the list, or go in the middle!
// const insertRLEList = <T>(list: T[], newItem: T, getKey: (e: T) => number, tryAppend: (a: T, b: T) => boolean) => {
//   const newKey = getKey(newItem)
//   if (list.length === 0 || newKey >= getKey(list[list.length - 1])) {
//     // Common case. Just push the new entry to the end of the list like normal.
//     pushRLEList(tryAppend, list, newItem)
//   } else {
//     // We need to splice the new entry in. Find the index of the previous entry...
//     let idx = bs(list, newKey, (entry, needle) => getKey(entry) - needle)
//     if (idx >= 0) throw Error('Invalid state - item already exists')

//     idx = ~idx // The destination index is the 2s compliment of the returned index.

//     // Try to append.
//     if (idx === 0 || !tryAppend(list[idx - 1], newItem)) {
//       // No good! Splice in.
//       list.splice(idx, 0, newItem)
//     }
//   }
// }

const tryRangeAppend = (r1: LVRange, r2: LVRange): boolean => {
  if (r1[1] === r2[0]) {
    r1[1] = r2[1]
    return true
  } else return false
}

const tryRevRangeAppend = (r1: LVRange, r2: LVRange): boolean => {
  if (r1[0] === r2[1]) {
    r1[0] = r2[0]
    return true
  } else return false
}

/** Sort in ascending order. */
const sortVersions = (v: LV[]): LV[] => v.sort((a, b) => a - b)

export const advanceFrontier = (frontier: LV[], vLast: LV, parents: LV[]): LV[] => {
  // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
  // for (const parent of op.parents) {
  //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
  // }

  const f = frontier.filter(v => !parents.includes(v))
  f.push(vLast)
  return sortVersions(f)
}

export const clientEntriesForAgent = (causalGraph: CausalGraphInner, agent: string): ClientEntry[] => (
  causalGraph.agentToVersion[agent] ??= []
)

const lastOr = <T, V>(list: T[], f: (t: T) => V, def: V): V => (
  list.length === 0 ? def : f(list[list.length - 1])
)

export const nextLV = (cg: CausalGraphInner): LV => (
  lastOr(cg.entries, e => e.vEnd, 0)
)

export const nextSeqForAgent = (cg: CausalGraphInner, agent: string): number => {
  const entries = cg.agentToVersion[agent]
  if (entries == null) return 0
  return entries[entries.length - 1].seqEnd
}

const tryAppendEntries = (a: CGEntry, b: CGEntry): boolean => {
  const canAppend = b.version === a.vEnd
    && a.agent === b.agent
    && a.seq + (a.vEnd - a.version) === b.seq
    && b.parents.length === 1 && b.parents[0] === a.vEnd - 1

  if (canAppend) {
    a.vEnd = b.vEnd
  }

  return canAppend
}

const tryAppendClientEntry = (a: ClientEntry, b: ClientEntry): boolean => {
  const canAppend = b.seq === a.seqEnd
    && b.version === (a.version + (a.seqEnd - a.seq))

  if (canAppend) {
    a.seqEnd = b.seqEnd
  }
  return canAppend
}

/** Returns idx (0+) if exists. Otherwise returns ~idx. */
const findAVIndex = (av: ClientEntry[], seq: number): number => (
  bs(av, seq, (entry, needle) => (
    needle < entry.seq ? 1
      : needle >= entry.seqEnd ? -1
      : 0
  ))
)

const findClientEntryRaw = (cg: CausalGraphInner, agent: string, seq: number): ClientEntry | null => {
  const av = cg.agentToVersion[agent]
  if (av == null) return null

  const idx = findAVIndex(av, seq)
  return idx < 0 ? null : av[idx]
}

const findClientEntry = (cg: CausalGraphInner, agent: string, seq: number): [ClientEntry, number] | null => {
  const clientEntry = findClientEntryRaw(cg, agent, seq)
  return clientEntry == null ? null : [clientEntry, seq - clientEntry.seq]
}

const findClientEntryTrimmed = (cg: CausalGraphInner, agent: string, seq: number): ClientEntry | null => {
  const result = findClientEntry(cg, agent, seq)
  if (result == null) return null

  const [clientEntry, offset] = result
  return offset === 0 ? clientEntry : {
    seq,
    seqEnd: clientEntry.seqEnd,
    version: clientEntry.version + offset
  }
}

export const hasVersion = (cg: CausalGraphInner, agent: string, seq: number): boolean => (
  findClientEntryRaw(cg, agent, seq) != null
)

// export const addLocal = (cg: CausalGraph, id: RawVersion, len: number = 1): LV => {
//   return add(cg, id[0], id[1], id[1]+len, cg.version)
// }

/** Returns the first new version in the inserted set */
export const addRemote = (cg: CausalGraphInner, id: Id, len: number = 1, rawParents?: Id[]): number => {
  const parents = rawParents != null
    ? idToLVList(cg, rawParents)
    : cg.heads

  return add(cg, id[0], id[1], id[1]+len, parents)
}

// Internal method used by add. This just adds things to cg.entries and updates cg.heads.
const internalAddToEntries = (cg: CausalGraphInner, version: LV, agent: string, seqStart: number, seqEnd: number, parents: LV[]): number => {
  const len = seqEnd - seqStart
  assert(len > 0)
  const vEnd = version + len

  const entry: CGEntry = {
    version,
    vEnd,

    agent,
    seq: seqStart,
    parents,
  }

  // The entry list will remain ordered here in standard version order.
  pushRLEList(tryAppendEntries, cg.entries, entry)

  if (parents === cg.heads) {
    // Short circuit optimisation.
    cg.heads = [vEnd - 1]
  } else {
    cg.heads = advanceFrontier(cg.heads, vEnd - 1, parents)
  }

  return len
}

/** Add an item to the causal graph. Unlike addRaw, this takes parents using LV[].
 *
 * Returns the number of items actually added. This could be fewer than requested (or zero)
 * if the causal graph already contains those elements.
 */
function add(cg: CausalGraphInner, agent: string, seqStart: number, seqEnd: number, parents: LV[] = cg.heads): number {
  if (seqStart === seqEnd) return 0 // Nothing to do in this case. Immediately return.

  const version = nextLV(cg)

  // This is quite subtle. There's 3 cases here:
  // 1. The new span is entirely known in the causal graph. Discard it.
  // 2. The new span is entirely unknown in the causal graph. This is the most likely case.
  //    Append all of it.
  // 3. There's some overlap. The overlap must be at the start of the entry, because all of
  //    each item's parents must be known.
  let av = cg.agentToVersion[agent]
  if (av) { // If av is null, we'll keep the whole thing.
    let idx = findAVIndex(av, seqEnd - 1)

    if (idx >= 0) return 0 // If we have the last seq, therefore we have the whole thing.

    idx = ~idx // Idx is now the "target index" - or, where in the list the item would be inserted.
    if (idx >= 1) {
      let prev = av[idx - 1]

      if (prev && prev.seqEnd > seqStart) {
        // We already have some of the changes.
        assert(prev.seqEnd < seqEnd) // Invalid - We would have gotten a 0+ index in this case.

        // Trim the inserted item.
        seqStart = prev.seqEnd
        // Parents = last item in previous entry.
        const prevEndLV = prev.version + (prev.seqEnd - prev.seq)
        parents = [prevEndLV - 1]

        // If (and only if) the versions match up, we can just extend the existing entry.
        if (prev.version + (prev.seqEnd - prev.seq) === version) {
          prev.seqEnd = seqEnd

          // Insert the CGEntry and update heads and we're done.
          return internalAddToEntries(cg, version, agent, seqStart, seqEnd, parents)
        }
      }
    }

    // Otherwise splice in at destination index. Note seqStart may be trimmed.
    const newEntry: ClientEntry = {
      seq: seqStart,
      seqEnd,
      version
    }
    if (idx === 0 || !tryAppendClientEntry(av[idx - 1], newEntry)) {
      av.splice(idx, 0, newEntry)
    }
  } else {
    // av = cg.agentToVersion[agent] = []
    cg.agentToVersion[agent] = [{
      seq: seqStart, seqEnd, version
    }]
  }

  return internalAddToEntries(cg, version, agent, seqStart, seqEnd, parents)
}
// /** Add an item to the causal graph. Unlike addRaw, this takes parents using LV[].
//  *
//  * Returns null if the entire span already exists in the causal graph.
//  */
// function add(cg: CausalGraphInner, agent: string, seqStart: number, seqEnd: number, parents: LV[] = cg.heads): LVRange | null {
//   if (seqStart === seqEnd) return null // Nothing to do in this case. Immediately return.

//   const version = nextLV(cg)

//   // This is quite subtle. There's 3 cases here:
//   // 1. The new span is entirely known in the causal graph. Discard it.
//   // 2. The new span is entirely unknown in the causal graph. This is the most likely case.
//   //    Append all of it.
//   // 3. There's some overlap. The overlap must be at the start of the entry, because all of
//   //    each item's parents must be known.
//   let av = cg.agentToVersion[agent]
//   if (av) { // If av is null, we'll keep the whole thing.
//     let idx = findAVIndex(av, seqEnd - 1)

//     if (idx >= 0) return null // If we have the last seq, therefore we have the whole thing.

//     idx = ~idx // Idx is now the "target index" - or, where in the list the item would be inserted.
//     if (idx >= 1) {
//       let prev = av[idx - 1]

//       if (prev && prev.seqEnd >= seqStart) {
//         // We already have some of the changes.
//         assert(prev.seqEnd < seqEnd) // Invalid - We would have gotten a 0+ index in this case.

//         // Trim the inserted item.
//         seqStart = prev.seqEnd
//         // Parents = last item in previous entry.
//         const prevEndLV = prev.version + (prev.seqEnd - prev.seq)
//         parents = [prevEndLV - 1]

//         // If (and only if) the versions match up, we can just extend the existing entry.
//         if (prev.version + (prev.seqEnd - prev.seq) === version) {
//           prev.seqEnd = seqEnd

//           // Insert the CGEntry and update heads and we're done.
//           return internalAddToEntries(cg, version, agent, seqStart, seqEnd, parents)
//         }
//       }
//     }

//     // Otherwise splice in at destination index. Note seqStart may be trimmed.
//     av.splice(idx, 0, {
//       seq: seqStart,
//       seqEnd,
//       version
//     })
//   } else {
//     // av = cg.agentToVersion[agent] = []
//     cg.agentToVersion[agent] = [{
//       seq: seqStart, seqEnd, version
//     }]
//   }

//   return internalAddToEntries(cg, version, agent, seqStart, seqEnd, parents)
// }

export const rawVersionCmp = ([a1, s1]: Id, [a2, s2]: Id) => (
  a1 < a2 ? -1
    : a1 > a2 ? 1
    : s1 - s2
)

export const lvCmp = (cg: CausalGraphInner, a: LV, b: LV) => (
  rawVersionCmp(lvToId(cg, a), lvToId(cg, b))
)

// export const tieBreakVersions = (cg: CausalGraph, data: LV[]): LV => {
//   if (data.length === 0) throw Error('Cannot tie break from an empty set')
//   let winner = data.reduce((a, b) => {
//     // Its a bit inefficient doing this lookup multiple times for the winning item,
//     // but eh. The data set will almost always contain exactly 1 item anyway.
//     const rawA = lvToRaw(cg, a)
//     const rawB = lvToRaw(cg, b)

//     return versionCmp(rawA, rawB) < 0 ? a : b
//   })

//   return winner
// }

/**
 * Returns [seq, local version] for the new item (or the first item if num > 1).
 */
export const assignLocal = (cg: CausalGraphInner, agentId: string, seq: number, parents: LV[] = cg.heads, num: number = 1): LV => {
  let version = nextLV(cg)
  const av = clientEntriesForAgent(cg, agentId)
  const nextValidSeq = lastOr(av, ce => ce.seqEnd, 0)
  if (seq < nextValidSeq) throw Error('Invalid agent seq')
  add(cg, agentId, seq, seq + num, parents)

  return version
}

export const findEntryContainingRaw = (cg: CausalGraphInner, v: LV): CGEntry => {
  const idx = bs(cg.entries, v, (entry, needle) => (
    needle < entry.version ? 1
    : needle >= entry.vEnd ? -1
    : 0
  ))
  if (idx < 0) throw Error('Invalid or unknown local version ' + v)
  return cg.entries[idx]
}
export const findEntryContaining = (cg: CausalGraphInner, v: LV): [CGEntry, number] => {
  const e = findEntryContainingRaw(cg, v)
  const offset = v - e.version
  return [e, offset]
}

export const lvToIdWithParents = (cg: CausalGraphInner, v: LV): [string, number, LV[]] => {
  const [e, offset] = findEntryContaining(cg, v)
  const parents = offset === 0 ? e.parents : [v-1]
  return [e.agent, e.seq + offset, parents]
}

export const lvToId = (cg: CausalGraphInner, v: LV): Id => {
  const [e, offset] = findEntryContaining(cg, v)
  return [e.agent, e.seq + offset]
  // causalGraph.entries[localIndex]
}

export const lvToIdList = (cg: CausalGraphInner, parents: LV[] = cg.heads): Id[] => (
  parents.map(v => lvToId(cg, v))
)


// export const getParents = (cg: CausalGraph, v: LV): LV[] => (
//   localVersionToRaw(cg, v)[2]
// )

export const tryRawToLV = (cg: CausalGraphInner, agent: string, seq: number): LV | null => {
  const clientEntry = findClientEntryTrimmed(cg, agent, seq)
  return clientEntry?.version ?? null
}
export const idToLV = (cg: CausalGraphInner, agent: string, seq: number): LV => {
  const clientEntry = findClientEntryTrimmed(cg, agent, seq)
  if (clientEntry == null) throw Error(`Unknown ID: (${agent}, ${seq})`)
  return clientEntry.version
}
export const rawToLV2 = (cg: CausalGraphInner, v: Id): LV => (
  idToLV(cg, v[0], v[1])
)

export const idToLVList = (cg: CausalGraphInner, ids: Id[]): LV[] => (
  ids.map(([agent, seq]) => idToLV(cg, agent, seq))
)

//! Returns LV at start and end of the span.
export const rawToLVSpan = (cg: CausalGraphInner, agent: string, seq: number): [LV, LV] => {
// export const rawToLVSpan = (cg: CausalGraph, agent: string, seq: number): [LV, number] => {
  const e = findClientEntry(cg, agent, seq)
  if (e == null) throw Error(`Unknown ID: (${agent}, ${seq})`)
  const [entry, offset] = e

  return [entry.version + offset, entry.seqEnd - entry.seq + entry.version] // [start, end]
  // return [entry.version + offset, entry.seqEnd - entry.seq - offset] // [start, len].
}

export const summarizeVersion = (cg: CausalGraphInner): VersionSummary => {
  const result: VersionSummary = {}
  for (const k in cg.agentToVersion) {
    const av = cg.agentToVersion[k]
    if (av.length === 0) continue

    const versions: [number, number][] = []
    for (const ce of av) {
      pushRLEList(tryRangeAppend, versions, [ce.seq, ce.seqEnd])
    }

    result[k] = versions
  }
  return result
}

export function eachVersionBetween(cg: CausalGraphInner, vStart: LV, vEnd: LV, visit: (e: CGEntry, vs: number, ve: number) => void) {
  let idx = bs(cg.entries, vStart, (entry, needle) => (
    needle < entry.version ? 1
    : needle >= entry.vEnd ? -1
    : 0
  ))
  if (idx < 0) throw Error('Invalid or missing version: ' + vStart)

  for (; idx < cg.entries.length; idx++) {
    const entry = cg.entries[idx]
    if (entry.version >= vEnd) break

    // const offset = max2(vStart - entry.version, 0)
    visit(entry, max2(vStart, entry.version), min2(vEnd, entry.vEnd))
  }
}

// Same as above, but as a generator. And generating a new CGEntry when we yield.
export function *iterVersionsBetween(cg: CausalGraphInner, vStart: LV, vEnd: LV): Generator<CGEntry> {
  if (vStart === vEnd) return

  let idx = bs(cg.entries, vStart, (entry, needle) => (
    needle < entry.version ? 1
    : needle >= entry.vEnd ? -1
    : 0
  ))
  // console.log('cg', cg.entries, vStart, vEnd)
  if (idx < 0) throw Error('Invalid or missing version: ' + vStart)

  for (; idx < cg.entries.length; idx++) {
    const entry = cg.entries[idx]
    if (entry.version >= vEnd) break

    if (vStart <= entry.version && vEnd >= entry.vEnd) {

      if (entry.version === entry.vEnd) throw Error('Invalid state')

      yield entry // Keep the entire entry.
    } else {
      // Slice the entry by vStart / vEnd.
      const vLocalStart = max2(vStart, entry.version)
      const vLocalEnd = min2(vEnd, entry.vEnd)

      if (vLocalStart === vLocalEnd) throw Error('Invalid state')

      yield {
        version: vLocalStart,
        vEnd: vLocalEnd,
        agent: entry.agent,
        seq: entry.seq + (vLocalStart - entry.version),
        parents: vLocalStart === entry.version ? entry.parents : [vLocalStart - 1],
      }
    }
  }
}
// interface VisitEntry {
//   entry: CGEntry,
//   vStart: LV,
//   vEnd: LV,
// }

// export function *iterVersionsBetween(cg: CausalGraph, vStart: LV, vEnd: LV): Generator<VisitEntry> {
//   let idx = bs(cg.entries, vStart, (entry, needle) => (
//     needle < entry.version ? 1
//     : needle >= entry.vEnd ? -1
//     : 0
//   ))
//   if (idx < 0) throw Error('Invalid or missing version: ' + vStart)

//   for (; idx < cg.entries.length; idx++) {
//     const entry = cg.entries[idx]
//     if (entry.version >= vEnd) break

//     // const offset = max2(vStart - entry.version, 0)
//     yield {
//       entry,
//       vStart: max2(vStart, entry.version),
//       vEnd: min2(vEnd, entry.vEnd)
//     }
//   }
// }

/** version is -1 when the seq does not overlap. Each yield is guaranteed to be a version run. */
type IntersectVisitor = (agent: string, startSeq: number, endSeq: number, version: number) => void

/**
 * Scan the VersionSummary and report (via visitor function) which versions overlap.
 *
 * If you consider the venn diagram of versions, there are 3 categories:
 * - a (only known locally)
 * - a+b (common versions)
 * - b (only known remotely)
 *
 * Currently this method:
 * - Ignores a only. Only a+b or b are yielded via the visitor
 * - For a+b, we yield the local version
 * - For b only, we yield a LV of -1.
 */
const intersectWithSummaryFull = (cg: CausalGraphInner, summary: VersionSummary, visit: IntersectVisitor) => {
  for (const agent in summary) {
    const clientEntries = cg.agentToVersion[agent]

    for (let [startSeq, endSeq] of summary[agent]) {
      // This is a bit tricky, because a single item in ClientEntry might span multiple
      // entries.

      if (clientEntries != null) { // Else no intersection here.
        let idx = bs(clientEntries, startSeq, (entry, needle) => (
          needle < entry.seq ? 1
            : needle >= entry.seqEnd ? -1
            : 0
        ))

        // If startSeq isn't found, start at the next entry.
        if (idx < 0) idx = -idx - 1

        for (; idx < clientEntries.length; idx++) {
          const ce = clientEntries[idx]
          if (ce.seq >= endSeq) break

          if (ce.seq > startSeq) {
            visit(agent, startSeq, ce.seq, -1)
            startSeq = ce.seq
          }

          const seqOffset = startSeq - ce.seq
          const versionStart = ce.version + seqOffset

          const localSeqEnd = min2(ce.seqEnd, endSeq)

          visit(agent, startSeq, localSeqEnd, versionStart)

          startSeq = localSeqEnd
        }
      }

      // More items known for this agent in the local cg than the remote one.
      if (startSeq < endSeq) visit(agent, startSeq, endSeq, -1)
    }
  }

  // // But if we're visiting the items only we know about, we need to scan all the locally known
  // // agents...
  // for (const agent in cg.agentToVersion) {
  //   if (summary[agent] != null) continue // Already covered above.

  //   const av = cg.agentToVersion[agent]
  //   // if (av.length === 0) continue

  //   // const versions: [number, number][] = []
  //   for (const ce of av) {
  //     visit(agent, ce.seq, ce.seqEnd, -1)
  //   }
  // }
}

/** Yields the intersection (most recent common version) and remainder (if any) */
export const intersectWithSummary = (cg: CausalGraphInner, summary: VersionSummary, versionsIn: LV[] = []): [LV[], VersionSummary | null] => {
  let remainder: null | VersionSummary = null

  const versions = versionsIn.slice()
  intersectWithSummaryFull(cg, summary, (agent, startSeq, endSeq, versionStart) => {
    if (versionStart >= 0) {
      const versionEnd = versionStart + (endSeq - startSeq)

      // Ok, now we go through everything from versionStart to versionEnd! Wild.
      eachVersionBetween(cg, versionStart, versionEnd, (e, vs, ve) => {
        const vLast = ve - 1
        if (vLast < e.version) throw Error('Invalid state')
        versions.push(vLast)
      })
    } else {
      remainder ??= {}
      const a = (remainder[agent] ??= [])
      a.push([startSeq, endSeq])
    }
  })

  return [findDominators(cg, versions), remainder]
}

// *** TOOLS ***

type DiffResult = {
  // These are ranges. Unlike the rust code, they're in normal
  // (ascending) order.
  aOnly: LVRange[], bOnly: LVRange[]
}

const pushReversedRLE = (list: LVRange[], start: LV, end: LV) => {
  pushRLEList(tryRevRangeAppend, list, [start, end] as [number, number])
}


// Numerical values used by utility methods below.
export const enum DiffFlag { A=0, B=1, Shared=2 }

export function diff(cg: CausalGraphInner, a: LV[], b: LV[]): DiffResult {
  const aOnly: LVRange[] = [], bOnly: LVRange[] = []

  diff2(cg, a, b, (start, end, inB) => {
    const target = inB ? bOnly : aOnly
    pushReversedRLE(target, start, end)
  })

  aOnly.reverse()
  bOnly.reverse()
  return {aOnly, bOnly}
}

// export type RangeHander = (start: number, end: number) => void
/**
 * This method takes in two versions (expressed as frontiers) and returns the
 * set of operations only appearing in the history of one version or the other.
 */
export function diff2(cg: CausalGraphInner, a: LV[], b: LV[], visitor: (start: number, end: number, inB: boolean) => void) {
  const flags = new Map<number, DiffFlag>()

  // Every order is in here at most once. Every entry in the queue is also in
  // itemType.
  const queue = new PriorityQueue<number>()

  // Number of items in the queue in both transitive histories (state Shared).
  let numShared = 0

  const enq = (v: LV, flag: DiffFlag) => {
    // console.log('enq', v, flag)
    const currentType = flags.get(v)
    if (currentType == null) {
      queue.enq(v)
      flags.set(v, flag)
      // console.log('+++ ', order, type, getLocalVersion(db, order))
      if (flag === DiffFlag.Shared) numShared++
    } else if (flag !== currentType && currentType !== DiffFlag.Shared) {
      // This is sneaky. If the two types are different they have to be {A,B},
      // {A,Shared} or {B,Shared}. In any of those cases the final result is
      // Shared. If the current type isn't shared, set it as such.
      flags.set(v, DiffFlag.Shared)
      numShared++
    }
  }

  for (const v of a) enq(v, DiffFlag.A)
  for (const v of b) enq(v, DiffFlag.B)

  // console.log('QF', queue, flags)

  const markRun = (start: LV, endInclusive: LV, flag: DiffFlag) => {
    if (endInclusive < start) throw Error('end < start')

    // console.log('markrun', start, end, flag)
    if (flag == DiffFlag.Shared) return

    // const target = flag === DiffFlag.A ? aOnly : bOnly
    // target(start, endInclusive + 1)
    visitor(start, endInclusive + 1, flag === DiffFlag.B)
  }

  // Loop until everything is shared.
  while (queue.size() > numShared) {
    let v = queue.deq()
    let flag = flags.get(v)!
    // It should be safe to remove the item from itemType here.

    // console.log('--- ', v, 'flag', flag, 'shared', numShared, 'num', queue.size())
    if (flag == null) throw Error('Invalid type')

    if (flag === DiffFlag.Shared) numShared--

    const e = findEntryContainingRaw(cg, v)
    // console.log(v, e)

    // We need to check if this entry contains the next item in the queue.
    while (!queue.isEmpty() && queue.peek() >= e.version) {
      const v2 = queue.deq()
      const flag2 = flags.get(v2)!
      // console.log('pop', v2, flag2)
      if (flag2 === DiffFlag.Shared) numShared--;

      if (flag2 !== flag) { // Mark from v2..=v and continue.
        // v2 + 1 is correct here - but you'll probably need a whiteboard to
        // understand why.
        markRun(v2 + 1, v, flag)
        v = v2
        flag = DiffFlag.Shared
      }
    }

    // console.log(e, v, flag)
    markRun(e.version, v, flag)

    for (const p of e.parents) enq(p, flag)
  }
}


/** Does frontier contain target? */
export const versionContainsLV = (cg: CausalGraphInner, frontier: LV[], target: LV): boolean => {
  if (frontier.includes(target)) return true

  const queue = new PriorityQueue<number>()
  for (const v of frontier) if (v > target) queue.enq(v)

  while (queue.size() > 0) {
    const v = queue.deq()
    // console.log('deq v')

    // TODO: Will this ever hit?
    if (v === target) return true

    const e = findEntryContainingRaw(cg, v)
    if (e.version <= target) return true

    // Clear any queue items pointing to this entry.
    while (!queue.isEmpty() && queue.peek() >= e.version) {
      queue.deq()
    }

    for (const p of e.parents) {
      if (p === target) return true
      else if (p > target) queue.enq(p)
    }
  }

  return false
}

/** Find the dominators amongst the input versions.
 *
 * Each item in the input will be output to the callback function exactly once.
 *
 * If a version is repeated, it will only ever be counted as a dominator once.
 *
 * The versions will be yielded from largest to smallest.
 */
export function findDominators2(cg: CausalGraphInner, versions: LV[], cb: (v: LV, isDominator: boolean) => void) {
  if (versions.length === 0) return
  else if (versions.length === 1) {
    cb(versions[0], true)
    return
  }
  else if (versions.length === 2) {
    // We can delegate to versionContainsLV, which is simpler.
    // TODO: Check if this fast path actually helps at all.
    let [v0, v1] = versions
    if (v0 === v1) {
      cb(v0, true)
      cb(v0, false)
    } else {
      if (v0 > v1) [v0, v1] = [v1, v0]
      // v0 < v1. So v1 must be a dominator.
      cb(v1, true)
      // I could use compareVersions, but we'll always hit the same case there.
      cb(v0, !versionContainsLV(cg, [v1], v0))
    }
    return
  }

  // The queue contains (version, isInput) pairs encoded using even/odd numbers.
  const queue = new PriorityQueue<number>()
  for (const v of versions) queue.enq(v * 2)

  let inputsRemaining = versions.length

  while (queue.size() > 0 && inputsRemaining > 0) {
    const vEnc = queue.deq()
    const isInput = (vEnc % 2) === 0
    const v = vEnc >> 1

    if (isInput) {
      cb(v, true)
      inputsRemaining -= 1
    }

    const e = findEntryContainingRaw(cg, v)

    // Clear any queue items pointing to this entry.
    while (!queue.isEmpty() && queue.peek() >= e.version * 2) {
      const v2Enc = queue.deq()
      const isInput2 = (v2Enc % 2) === 0
      if (isInput2) {
        cb(v2Enc >> 1, false)
        inputsRemaining -= 1
      }
    }

    for (const p of e.parents) {
      queue.enq(p * 2 + 1)
    }
  }
}

export function findDominators(cg: CausalGraphInner, versions: LV[]): LV[] {
  if (versions.length <= 1) return versions
  const result: LV[] = []
  findDominators2(cg, versions, (v, isDominator) => {
    if (isDominator) result.push(v)
  })
  return result.reverse()
}

export const lvEq = (a: LV[], b: LV[]) => (
  a.length === b.length && a.every((val, idx) => b[idx] === val)
)

export function findConflicting(cg: CausalGraphInner, a: LV[], b: LV[], visit: (range: LVRange, flag: DiffFlag) => void): LV[] {
  // dbg!(a, b);

  // Sorted highest to lowest (so we get the highest item first).
  type TimePoint = {
    v: LV[], // Sorted in inverse order (highest to lowest)
    flag: DiffFlag
  }

  const pointFromVersions = (v: LV[], flag: DiffFlag) => ({
    v: v.length <= 1 ? v : v.slice().sort((a, b) => b - a),
    flag
  })

  // The heap is sorted such that we pull the highest items first.
  // const queue: BinaryHeap<(TimePoint, DiffFlag)> = BinaryHeap::new();
  const queue = new PriorityQueue<TimePoint>((a, b) => {
    for (let i = 0; i < a.v.length; i++) {
      if (b.v.length <= i) return 1
      const c = a.v[i] - b.v[i]
      if (c !== 0) return c
    }
    if (a.v.length < b.v.length) return -1

    return a.flag - b.flag
  })

  queue.enq(pointFromVersions(a, DiffFlag.A));
  queue.enq(pointFromVersions(b, DiffFlag.B));

  // Loop until we've collapsed the graph down to a single element.
  while (true) {
    let {v, flag} = queue.deq()
    // console.log('deq', v, flag)
    if (v.length === 0) return []

    // Discard duplicate entries.

    // I could write this with an inner loop and a match statement, but this is shorter and
    // more readable. The optimizer has to earn its keep somehow.
    // while queue.peek() == Some(&time) { queue.pop(); }
    while (!queue.isEmpty()) {
      const {v: peekV, flag: peekFlag} = queue.peek()
      // console.log('peek', peekV, v, lvEq(v, peekV))
      if (lvEq(v, peekV)) {
        if (peekFlag !== flag) flag = DiffFlag.Shared
        queue.deq()
      } else break
    }

    if (queue.isEmpty()) return v.reverse()

    // If this node is a merger, shatter it.
    if (v.length > 1) {
      // We'll deal with v[0] directly below.
      for (let i = 1; i < v.length; i++) {
        // console.log('shatter', v[i], 'flag', flag)
        queue.enq({v: [v[i]], flag})
      }
    }

    const t = v[0]
    const containingTxn = findEntryContainingRaw(cg, t)

    // I want an inclusive iterator :p
    const txnStart = containingTxn.version
    let end = t + 1

    // Consume all other changes within this txn.
    while (true) {
      if (queue.isEmpty()) {
        return [end - 1]
      } else {
        const {v: peekV, flag: peekFlag} = queue.peek()
        // console.log('inner peek', peekV, (queue as any)._elements)

        if (peekV.length >= 1 && peekV[0] >= txnStart) {
          // The next item is within this txn. Consume it.
          queue.deq()
          // console.log('inner deq', peekV, peekFlag)

          const peekLast = peekV[0]

          // Only emit inner items when they aren't duplicates.
          if (peekLast + 1 < end) {
            // +1 because we don't want to include the actual merge point in the returned set.
            visit([peekLast + 1, end], flag)
            end = peekLast + 1
          }

          if (peekFlag !== flag) flag = DiffFlag.Shared

          if (peekV.length > 1) {
            // We've run into a merged item which uses part of this entry.
            // We've already pushed the necessary span to the result. Do the
            // normal merge & shatter logic with this item next.
            for (let i = 1; i < peekV.length; i++) {
              // console.log('shatter inner', peekV[i], 'flag', peekFlag)

              queue.enq({v: [peekV[i]], flag: peekFlag})
            }
          }
        } else {
          // Emit the remainder of this txn.
          // console.log('processed txn', txnStart, end, 'flag', flag, 'parents', containingTxn.parents)
          visit([txnStart, end], flag)

          queue.enq(pointFromVersions(containingTxn.parents, flag))
          break
        }
      }
    }
  }
}



/**
 * Two versions have one of 4 different relationship configurations:
 * - They're equal (a == b)
 * - They're concurrent (a || b)
 * - Or one dominates the other (a < b or b > a).
 *
 * This method depends on the caller to check if the passed versions are equal
 * (a === b). Otherwise it returns 0 if the operations are concurrent,
 * -1 if a < b or 1 if b > a.
 */
export const compareVersions = (cg: CausalGraphInner, a: LV, b: LV): number => {
  if (a > b) {
    return versionContainsLV(cg, [a], b) ? -1 : 0
  } else if (a < b) {
    return versionContainsLV(cg, [b], a) ? 1 : 0
  }
  throw new Error('a and b are equal')
}


// *** Tools to syncronize causal graphs ***

type PartialSerializedCGEntry = {
  agent: string,
  seq: number,
  len: number,

  parents: Id[]
}

export type PartialSerializedCG = PartialSerializedCGEntry[]

/**
 * The entries returned from this function are in the order of versions
 * specified in ranges.
 */
export function serializeDiff(cg: CausalGraphInner, ranges: LVRange[]): PartialSerializedCG {
  const entries: PartialSerializedCGEntry[] = []
  for (let [start, end] of ranges) {
    while (start != end) {
      const [e, offset] = findEntryContaining(cg, start)

      const localEnd = min2(end, e.vEnd)
      const len = localEnd - start
      const parents: Id[] = offset === 0
        ? lvToIdList(cg, e.parents)
        : [[e.agent, e.seq + offset - 1]]

      entries.push({
        agent: e.agent,
        seq: e.seq + offset,
        len,
        parents
      })

      start += len
    }
  }

  return entries
}

//! The entries returned from this function are always in causal order.
export function serializeFromVersion(cg: CausalGraphInner, v: LV[]): PartialSerializedCG {
  const ranges = diff(cg, v, cg.heads).bOnly
  return serializeDiff(cg, ranges)
}

export function mergePartialVersions(cg: CausalGraphInner, data: PartialSerializedCG): LVRange {
  const start = nextLV(cg)

  for (const {agent, seq, len, parents} of data) {
    addRemote(cg, [agent, seq], len, parents)
  }
  return [start, nextLV(cg)]
}

export function *mergePartialVersions2(cg: CausalGraphInner, data: PartialSerializedCG) {
  // const start = nextLV(cg)

  for (const {agent, seq, len, parents} of data) {
    const newEntry = addRemote(cg, [agent, seq], len, parents)
    if (newEntry != null) yield newEntry
  }

  // return [start, nextLV(cg)]
}

export function advanceVersionFromSerialized(cg: CausalGraphInner, data: PartialSerializedCG, version: LV[]): LV[] {
  for (const {agent, seq, len, parents} of data) {
    const parentLVs = idToLVList(cg, parents)
    const vLast = idToLV(cg, agent, seq + len - 1)
    version = advanceFrontier(version, vLast, parentLVs)
  }

  // NOTE: Callers might need to call findDominators on the result.
  return version
}

export function checkCG(cg: CausalGraphInner) {
  // There's a bunch of checks to put in here...
  for (let i = 0; i < cg.entries.length; i++) {
    const e = cg.entries[i]
    if (e.vEnd <= e.version) throw Error('Inverted versions in entry')
    // assert(e.vEnd > e.version)
  }

  // TODO: Also check the entry sequence matches the mapping.
}




// let cg = new CausalGraph()
// cg.add('x', 0, 5, [])
// cg.add('x', 4, 10, [123])

// console.log(cg.inner.entries)
// console.log(cg.inner.agentToVersion)

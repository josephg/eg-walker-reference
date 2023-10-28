import assert from 'node:assert/strict'
import seedRandom from 'seed-random'
import { ListOpLog, createOpLog, localDelete, localInsert, mergeOplogInto } from './oplog.js'
import { checkoutSimple } from './merge.js'
import consoleLib from 'console'
import { checkCG } from './causal-graph.js'


interface Doc {
  oplog: ListOpLog<number>,
  content: number[],
  agent: string,
}

const createDoc = (agent: string): Doc => {
  return { oplog: createOpLog(), content: [], agent }
}

const docInsert = (doc: Doc, pos: number, content: number) => {
  localInsert(doc.oplog, doc.agent, pos, content)
  doc.content.splice(pos, 0, content)
}

const docDelete = (doc: Doc, pos: number, len: number) => {
  localDelete(doc.oplog, doc.agent, pos, len)
  doc.content.splice(pos, len)
}

const docCheck = (doc: Doc) => {
  checkCG(doc.oplog.cg)
  const expectedContent = checkoutSimple(doc.oplog).data
  assert.deepEqual(expectedContent, doc.content)
}

const docMergeInto = (dest: Doc, src: Doc) => {
  mergeOplogInto(dest.oplog, src.oplog)

  // TODO: Use a fancier updating merge to reuse the existing content.
  dest.content = checkoutSimple(dest.oplog).data
}


function fuzzer(seed: number) {
  globalThis.console = new consoleLib.Console({
    stdout: process.stdout, stderr: process.stderr,
    inspectOptions: {depth: null}
  })

  const random = seedRandom(`zz ${seed}`)
  const randInt = (n: number) => Math.floor(random() * n)
  const randBool = (weight: number = 0.5) => random() < weight

  const docs = [createDoc('a'), createDoc('b'), createDoc('c')]
  const randDoc = () => docs[randInt(docs.length)]

  let nextItem = 0

  for (let i = 0; i < 100; i++) {
    // Generate some random operations
    for (let j = 0; j < 3; j++) {
      const doc = randDoc()

      const len = doc.content.length
      const insWeight = len < 100 ? 0.6 : 0.4
      if (len === 0 || randBool(insWeight)) {
        // Insert!
        const content = ++nextItem
        let pos = randInt(len + 1)
        docInsert(doc, pos, content)
      } else {
        const pos = randInt(len)
        const delLen = randInt(Math.min(len - pos, 3)) + 1
        // console.log('delete', pos, delLen)
        docDelete(doc, pos, delLen)
      }

      docCheck(doc) // EXPENSIVE
    }

    // Pick a random pair of documents and merge them
    const a = randDoc()
    const b = randDoc()
    if (a !== b) {
      // console.log('a', a, 'b', b)

      docMergeInto(a, b)
      // console.log(a)
      // debugger
      docMergeInto(b, a)
      // console.log(b)
      // console.log('a', a.content, 'b', b.content)
      // console.log('a', a, 'b', b)
      assert.deepEqual(a.content, b.content)
    }
  }
}

function fuzzLots() {
  for (let i = 0; i < 100000; i++) {
    if (i % 10 === 0) console.log('i', i)
    try {
      fuzzer(i)
    } catch (e) {
      console.log('in seed', i)
      throw e
    }
  }
}

// fuzzer(58)
fuzzLots()
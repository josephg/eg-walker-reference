// This is a port from the fugue repository Oct 2023
// Commit 98c0c7a965276fb9a22237562f642a6ce8d8e03f

export interface ID {
  sender: string;
  counter: number;
}

const idEq = (a: ID | null | undefined, b: ID | null): boolean => (
  a == b || (
    a != null && b != null
    && a.sender === b.sender && a.counter === b.counter
  )
)

const idCmp = ({sender: a1, counter: s1}: ID, {sender: a2, counter: s2}: ID) => (
  a1 < a2 ? -1
    : a1 > a2 ? 1
    : s1 - s2
)


interface Element<T> {
  /** For the start & end, this is ("", 0) & ("", 1). */
  id: ID;
  value: T | null;
  isDeleted: boolean;
  /** null for start and end. */
  leftOrigin: Element<T> | null;
  /** null for start and end. */
  rightOrigin: Element<T> | null;
  /** Linked list structure: the element currently to our left. */
  left: Element<T> | null;
  /** Linked list structure: the element currently to our right. */
  right: Element<T> | null;
}

export interface InsertMessage<T> {
  type: "insert";
  id: ID;
  value: T;
  leftOrigin: ID;
  rightOrigin: ID;
}

export interface DeleteMessage {
  type: "delete";
  id: ID;
}

export type Message<T> = InsertMessage<T> | DeleteMessage

export class ListFugueSimple<T> {
  readonly start: Element<T>;
  readonly end: Element<T>;

  replicaId: string;
  counter = 0;

  /**
   * Used in getByID.
   *
   * Map from ID.sender, to an array that maps ID.counter, to element with that ID.
   */
  readonly elementsByID = new Map<string, Element<T>[]>();
  /** Cached length. */
  _length = 0;

  // All the elements we've seen, in causal order. This makes saving & loading
  // much more simple.
  msgsInCausalOrder: Message<T>[] = []


  constructor(replicaId: string) {
    this.replicaId = replicaId
    this.start = {
      id: { sender: "", counter: 0 },
      value: null,
      isDeleted: true,
      leftOrigin: null,
      rightOrigin: null,
      left: null,
      right: null,
    };
    this.end = {
      id: { sender: "", counter: 1 },
      value: null,
      isDeleted: true,
      leftOrigin: null,
      rightOrigin: null,
      left: this.start,
      right: null,
    };
    this.start.right = this.end;
    this.elementsByID.set("", [this.start, this.end]);
  }

  insert(index: number, ...values: T[]): T | undefined {
    for (let i = 0; i < values.length; i++) {
      this.insertOne(index + i, values[i]);
    }

    // The return value is just an interface requirement; not relevant here.
    return undefined;
  }

  private insertOne(index: number, value: T) {
    this.insertOneWithReplica(this.replicaId, this.counter, index, value)
    this.counter++
  }

  insertOneWithReplica(replicaId: string, counter: number, index: number, value: T) {
    // insert generator.
    const id = { sender: replicaId, counter };
    const leftOrigin = index === 0 ? this.start : this.getByIndex(index - 1);
    const rightOrigin = leftOrigin.right!;
    const msg: InsertMessage<T> = {
      type: "insert",
      id,
      value,
      leftOrigin: leftOrigin.id,
      rightOrigin: rightOrigin.id,
    };
    // Message is delivered to receivePrimitive (the effector).
    // super.sendPrimitive(JSON.stringify(msg));
    this.receivePrimitive(msg)
  }

  delete(startIndex: number, count = 1): void {
    for (let i = 0; i < count; i++) this.deleteOne(startIndex);
  }

  private deleteOne(index: number): void {
    // delete generator.
    const elt = this.getByIndex(index);
    const msg: DeleteMessage = { type: "delete", id: elt.id };
    // Message is delivered to receivePrimitive (the effector).
    // super.sendPrimitive(JSON.stringify(msg));
    this.receivePrimitive(msg)
  }

  receivePrimitive(msg: Message<T>): void {
    // const msg: InsertMessage<T> | DeleteMessage = JSON.parse(<string>message);
    switch (msg.type) {
      case "insert": {
        // insert effector
        if (this.hasID(msg.id)) return // We already have this item.

        const leftOrigin = this.getByID(msg.leftOrigin);
        const rightOrigin = this.getByID(msg.rightOrigin);
        const left = this.computeLeft(msg.id, leftOrigin, rightOrigin);

        // Insert a new elt into the linked last after left.
        const right = left.right!;
        const elt: Element<T> = {
          id: msg.id,
          value: msg.value,
          isDeleted: false,
          leftOrigin,
          rightOrigin,
          left,
          right,
        };
        left.right = elt;
        right.left = elt;

        // Add elt to elementsByID.
        let bySender = this.elementsByID.get(msg.id.sender);
        if (bySender === undefined) {
          bySender = [];
          this.elementsByID.set(msg.id.sender, bySender);
        }
        // bySender.push(elt);
        bySender[msg.id.counter] = elt

        this._length++;

        // In a production implementation, we would emit an Insert event here.
        break;
      }
      case "delete": {
        // delete effector
        const elt = this.getByID(msg.id);
        if (elt.isDeleted) return

        elt.value = null;
        elt.isDeleted = true;
        this._length--;
        // In a production implementation, we would emit a Delete event here.
        break;
      }
      default:
        throw new Error("Bad message: " + msg);
    }

    // We fall through if the message hasn't been processed yet.
    this.msgsInCausalOrder.push(msg)
  }

  private computeLeft(
    id: ID,
    leftOrigin: Element<T>,
    rightOrigin: Element<T>
  ): Element<T> {
    const rightParent = this.rightParent(leftOrigin, rightOrigin);

    let left = leftOrigin;
    let scanning = false;

    // o ranges from leftOrigin to (non-adjusted) rightOrigin, *exclusive*.
    // Note that o will never be start or end (within the loop),
    // so its origins are non-null.
    for (let o = leftOrigin.right!; o !== rightOrigin; o = o.right!) {
      if (this.lessThan(o.leftOrigin!, leftOrigin)) break;
      else if (o.leftOrigin === leftOrigin) {
        const oRightParent = this.rightParent(o.leftOrigin, o.rightOrigin!);

        if (this.lessThan(oRightParent, rightParent)) {
          scanning = true;
        } else if (oRightParent === rightParent) {
          // o and the new elt are double siblings.
          if (idCmp(o.id, id) > 0) break;
          else scanning = false;
        } else {
          // oRightParent > rightParent
          scanning = false;
        }
      }

      if (!scanning) left = o;
    }

    return left;
  }

  private rightParent(leftOrigin: Element<T>, rightOrigin: Element<T>): Element<T> {
    if (rightOrigin === this.end || rightOrigin.leftOrigin !== leftOrigin) return this.end;
    else return rightOrigin;
  }

  /**
   * Returns whether a < b in the linked list order.
   */
  private lessThan(a: Element<T>, b: Element<T>): boolean {
    if (a === b) return false;
    // Loop forwards from each of a & b in parallel until one finds the other.
    // In principle this takes O(n) time, but in practice a & b should usually
    // be close together.
    let afterA = a;
    let afterB = b;
    while (true) {
      if (afterA === b || afterB.right === null) return true;
      if (afterB === a || afterA.right === null) return false;
      afterA = afterA.right;
      afterB = afterB.right;
    }
  }

  private hasID(id: ID): boolean {
    const bySender = this.elementsByID.get(id.sender);
    if (bySender == null) return false
    return bySender[id.counter] != null
  }

  private getByID(id: ID): Element<T> {
    const bySender = this.elementsByID.get(id.sender);
    if (bySender !== undefined) {
      const node = bySender[id.counter];
      if (node !== undefined) return node;
    }
    throw new Error("Unknown ID: " + JSON.stringify(id));
  }

  private getByIndex(index: number): Element<T> {
    if (index < 0 || index >= this.length) {
      throw new Error(
        "Index out of range: " + index + " (length: " + this.length + ")"
      );
    }

    // For now, do a slow linear search, but from the end b/c that's more common.
    // An easy common-case optimization is to cache index "hints" like in Yjs.
    // A doable aysmptotic optimization is to build a balanced tree structure
    // on top of the non-deleted list elements and use that to convert between
    // indices & elements in O(log(n)) time.
    let remaining = this.length - 1 - index;
    for (let elt = this.end.left!; elt !== this.start; elt = elt.left!) {
      if (!elt.isDeleted) {
        if (remaining === 0) return elt;
        remaining--;
      }
    }
    throw new Error("Index in range but not found");
  }

  get(index: number): T {
    return this.getByIndex(index).value!;
  }

  *values(): IterableIterator<T> {
    // Walk the linked list.
    for (
      let elt: Element<T> | null = this.start;
      (elt = elt.right);
      elt !== null
    ) {
      if (!elt.isDeleted) yield elt.value!;
    }
  }

  toArray(): T[] {
    return [...this.values()]
  }

  get length(): number {
    return this._length;
  }

  save(): Message<T>[] {
    // Save the linked list (less start & end) in causal order for easy merging.
    return this.msgsInCausalOrder
  }

  // load(bytes: Uint8Array): void {
  load(save: Message<T>[]): void {
    for (const msg of save) {
      this.receivePrimitive(msg)
    }
  }

  mergeFrom(other: ListFugueSimple<T>) {
    const save = other.save()
    this.load(save)
  }

  debugPrint() {
    // Walk the linked list.

    const depth: Record<string, number> = {}
    // const kForId = (id: Id, c: T | null) => `${id[0]} ${id[1]} ${id[2] ?? c != null}`
    const eltId = (elt: Element<any>) => elt.id.sender === '' ? 'ROOT' : `${elt.id.sender},${elt.id.counter}`
    depth[eltId(this.start)] = 0

    console.log('---- FUGUE STATE ----')
    for (
      let elt: Element<T> | null = this.start;
      (elt = elt.right);
      elt !== null
    ) {
      // The only items with a null left / right are the roots.
      if (elt.leftOrigin == null || elt.rightOrigin == null) continue

      const isLeftChild = true
      // const isLeftChild = this.rightParent(elt.leftOrigin, elt.rightOrigin) === this.end
      const parent = isLeftChild ? elt.leftOrigin : elt.rightOrigin
      const d = (parent === this.start || parent === this.end)
        ? 0
        : depth[eltId(parent)] + 1

      depth[eltId(elt)] = d

      // let content = `${isLeftChild ? '/' : '\\'}${elt.value == null
      let content = `${elt.value == null
        ? '.'
        // : elt.isDeleted ? chalk.strikethrough(elt.value) : chalk.yellow(elt.value)
        : elt.value
      } at [${eltId(elt)}] (left [${eltId(elt.leftOrigin)}])`
      content += ` right [${eltId(elt.rightOrigin)}]`
      content += ` rightParent ${eltId(this.rightParent(elt.leftOrigin, elt.rightOrigin))}`
      // console.log(`${'| '.repeat(d)}${elt.value == null ? chalk.strikethrough(content) : content}`)
      console.log(`${'| '.repeat(d)}${elt.value == null ? content : content}`)
    }



  }
}

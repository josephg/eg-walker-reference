

// We use higher numbers in production mode.

// export const NODE_CHILDREN = 4
// export const LEAF_CHILDREN = 4

/** Tree constants */
export const enum TC {
  NODE_CHILDREN = 32,
  LEAF_CHILDREN = 16,

  NODE_SPLIT_POINT = NODE_CHILDREN / 2,
  LEAF_SPLIT_POINT = LEAF_CHILDREN / 2,
}

// Type aliases just to make it a bit clearer what all the numbers represent.
export type LV = number
export type LeafIdx = number
export type NodeIdx = number

/**
 * When we have a next pointer / parent pointer which does not exist, we use this sentinal value
 */
export const NULL_IDX = -1

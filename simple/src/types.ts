// import type * as causalGraph from "./causal-graph.js"

import { LV } from "./causal-graph.js";


export interface Branch<T = any> {
  data: T[],
  version: LV[]
}

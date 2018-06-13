package com.github.flinkalt.typeinfo.instances

/**
  * A recursive object supporting the definition of inductive methods which don't cause a
  * StackOverflow. Cycles in the object graph are detected and handled explicitly.
  */
trait InductiveObject {

  // Marker for detecting cycles in the object graph.
  private var cycle = false

  /**
    * Avoids StackOverflow whenever a cycle in the object graph is expected.
    *
    * @param base Induction base (invoked when a cycle is detected).
    * @param step Induction step (invoked until a cycle is reached).
    * @tparam A The type of recursive computation.
    * @return The result of replacing cyclic references with `base` inside `step`.
    */
  protected def inductive[A](base: => A)(step: => A): A =
    if (cycle) {
      base
    } else {
      try {
        cycle = true
        step
      } finally {
        cycle = false
      }
    }
}

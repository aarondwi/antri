# antri
In-memory, asynchronous task queue implementation.
Primarily intended for learning to write a persistent message queue.

Trivia
------------------------------------------------------
**antri** is indonesian for `waiting in a queue`

Installation
------------------------------------------------------

1. Clone the repo, `git@github.com:aarondwi/antri.git`
2. Install go compiler on your platform
3. cd into the repo, and build with `go build .`

Usage
------------------------------------------------------

See [the code directly](https://github.com/aarondwi/antri/blob/master/antriserver_test.go) for usages

Features
-------------------------------------------------------

1. using grpc, easy to create client
2. individual and/or batch add/get/commit
3. at least once delivery (with task timeout)
4. task scheduling/visibility, e.g. how many seconds from commit before the task can be retrieved
5. durability (via wal, and periodic snapshotting)

Notes
------------------------------------------------------

1. Add/Commit Multiple Message is **NOT YET** atomic
2. **NOT** production ready

Possible optimization
------------------------------------------------------------------------

1. implement group fsync (based on time and/or number and/or size)

2. reduce lock contention. Wwhat has come to mind:

    * change pq and sl to use lock-free data structure
    * try simple array as internal DS + a b-tree/skiplist for index order, to reduce the number of swapping (also removes all usage of orderedmap)
    * Use multiple internal queue (but because pull-model, comes problem to decide how to get from those)
    * do batching on add/get/commit

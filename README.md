# antri
toy implementation of asynchronous task queue, primarily intended for learning

Trivia
------------------------------------------------------
**antri** is indonesian for `waiting in a queue`

Features
-------------------------------------------------------

1. using http as protocol (fasthttp) -> pull model
2. individual commit/reject
3. durability/at least once delivery (with task timeout)
4. task scheduling/visibility, e.g. how many seconds from commit before the task can be retrieved

To Do:
------------------------------------------------------

1. snapshot, for recovery -> following redis model

    * also need to use segmented log, so can skip checking faster

2. optional dead letter queue
3. optional cluster (raft / lock-service based)
4. docs how to use

Notes
------------------------------------------------------

1. intended for production? **yes**/**no**

    * yes as in should be simple enough to operate
    * yes, if you can expect a bit of bug here and there
    * yes, as this implementation should be fast enough for most case
    * no, as in no security mechanism provided

2. support priority? **NO**

    * separate queue for each priority (possible, but not now)

3. shared buffers? **no need** (for now)

    * the size should be small enough to fit even in memory of relatively small machine, and recovery is based on log

4. topic? **NO**

    * it will complicate the codebase (for now) as it is intended primarily for learning, and actually, you can run 1 instance as 1 topic :D

5. config file? later

    * this should be easy if the core logic is done

6. stats? later

    * I am also still thinking what stats should be provided :D, and probably gonna be using prometheus

Possible optimization/reliability
------------------------------------------------------------------------

1. batch file write using fsync (based on time and/or number)

    * currently using os.O_SYNC flag (so synced on every write), but on windows, the official golang compiler does not map O_SYNC to windows equivalent, so it is currently unsafe on windows.

2. find way to reduce lock contention when putting/taking from queue? what has come to mind:

    * change pq and sl to use ConcurrentSkipList + unroll the skiplist
    * try simple array as internal DS, but lost the schedule feature

3. sync.Pool to reduce allocation
4. Increase reliability of internal storage

    * use proper embedded database, e.g. rocksdb/badgerdb/innodb/others

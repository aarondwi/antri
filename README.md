# antri
toy implementation of scheduled-task queue, primarily intended for learning

Trivia
------------------------------------------------------
**antri** is indonesian for `waiting in a queue`

Features
-------------------------------------------------------

1. using http as protocol (fasthttp) -> pull model
2. individual commit/reject
3. durability/at least once delivery (with task timeout)

To Do:
------------------------------------------------------

1. snapshot, for recovery -> following redis model
2. optional dead letter queue
3. optional cluster (raft / lock-service based)
4. docs how to use

Notes
------------------------------------------------------

1. intended for production? **yes**/**no**

    * yes as in simple enough to operate, and if you can expect a but here in there (and this should be fast enough for most simple case)
    * no, as in no security mechanism provided

2. support priority? **NO**

    * separate queue for each priority (possible, but not now)

3. shared buffers? **no need** (for now)

    * the size should be small enough to fit even in memory of relatively small machine, and recovery is based on log

4. segment log? needed, but not top priority for now

    * easier to backup data

5. topic? **NO**

    * it will complicate the codebase (for now) as it is intended primarily for learning, and actually, you can run 1 instance as 1 topic :D

6. config file? later

    * this should be easy if the core logic is done

7. stats? later

    * I am also still thinking what stats should be provided :D, and probably gonna be using prometheus

Possible optimization
------------------------------------------------------------------------

1. batch file write using fsync (based on time and/or number)

    * currently using os.O_SYNC flag, but on windows, not all golang compiler map O_SYNC to windows equivalent, so it is currently unsafe on windows.

2. find way to reduce type-casting between string and []byte, inside `AddTask` (but need json representation as respond, and binary becomes base64)
3. find way to reduce lock contention when putting/taking from queue? what has come to mind:

    * change lock pattern with logical lock + latch
    * change pq and sl to use ConcurrentSkipList + unroll the skiplist
    * try simple array as internal DS, but lost the schedule feature
    * use proper embedded database, e.g. rocksdb/badgerdb

4. sync.Pool to reduce allocation

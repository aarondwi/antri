# antri
toy implementation of delayed-task queue, primarily intended for learning

Trivia
------------------------------------------------------
**antri** is indonesian for `waiting in line`

Features
-------------------------------------------------------

1. using http as protocol (fasthttp)
2. individual commit/reject
3. durability/at least once delivery (with task timeout)

To Do:
------------------------------------------------------

1. snapshot, for recovery -> following redis model
2. dead letter queue
3. optional cluster (raft / lock-service based)

Notes
------------------------------------------------------

1. intended for production? **yes**/**no**
    -> yes as in simple enough to operate, and if you can expect a but here in there (and this should be fast enough for most simple case)
    -> no, as in no security module provided
2. support priority? **NO**
    a. gonna need to handle indexng, and change the data format
    b. separate queue for each priority (possible, but not now)
3. shared buffers? **no need** (for now)
    -> the size should be small enough to fit even in memory of relatively small machine
    -> recovery based on log
4. segment log? needed, but not top priority for now
    -> easier to backup data
5. topic? **NO**
    -> it will complicate the codebase (for now) as it is intended primarily for learning, and actually, you can run 1 instance as 1 topic :D
6. config file? later
    -> this should be easy if the core logic is done
7. stats? later
    -> I am also still thinking what stats should be provided :D

Possible optimization
------------------------------------------------------------------------

0. batch fsync (based on time and number)
1. find way to reduce type-casting between string and []byte, inside `AddTask`
2. find way to reduce lock contention when putting/taking from queue? what has come to mind:
    -> change priorityqueue with something else than can be locked concurrently
    -> change lock apttern with logical lock + latch
3. find way so we don't have to pass entire PqItem into inflightRecords (or just sync.Pool probably?)

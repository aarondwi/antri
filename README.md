# antri
toy implementation of asynchronous task queue, primarily intended for learning

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

First, do the installation part, and run it with `./antri` or `.\antri.exe`. That will start the server at port 8080.

Then, you can start putting your task to the queue(with curl for example, cause via http)

```shell
curl -XPOST http://127.0.0.1:8080/add -d 'value=helloworld&secondsfromnow=0'
```

This will return the key of the created task (e.g. `AZ46xL8dbmiYrFT4`). The key is a 16-byte alphanumeric. This task is also sync to disk to ensure durability (currently with O_SYNC flag). Note the `secondsfromnow` part, it is to set at what time should the task starts to be visible (for retrieval by workers)

From your workers, you can retrieve the task with

```shell
curl http://127.0.0.1:8080/retrieve
```

This will return the json of the task, looks like below

```json
{
    "scheduledAt":1596445069,
    "key":"LJKftTj3N0gOGaJO",
    "value":"hello",
    "retries":0
}
```

By default, the task is given 10 seconds to commit/reject before considered `dead` (meaning, need to be rescheduled). This is to guarantee liveness property that all data must be sent (at-least-once guarantee).

If no task currently exists in the internal data structure, the `/retrieve` API will block until a task is present, whether from `/add` or the timeout of previous retrieval

After that, you can commit or reject(put it back to the queue, for now) your task like below

```shell
# for commit
curl -XPOST http://127.0.0.1:8080/{task key}/commit

# for reject
curl -XPOST http://127.0.0.1:8080/{task key}/reject
```

where task key is the attribute named `key` in the `/retrieve` API

Depending on the status of the task (it may timeout before you commit it) you can get the response below

```shell
# if success
OK

# if failed, probably cause of timeout or non-existent key
Task Key not found
```

If you commit the task and returns OK, the task key is also synced to disk to ensure durability, and it will not be returned again

Features
-------------------------------------------------------

1. using http as protocol (fasthttp) -> pull model (easy to create client api)
2. individual commit/reject
3. durability/at least once delivery (with task timeout)
4. task scheduling/visibility, e.g. how many seconds from commit before the task can be retrieved

To Do:
------------------------------------------------------

0. docs how to use
1. snapshot, for recovery -> following redis model

    * also need to use segmented log, so can skip checking faster

2. optional cluster (raft / lock-service based)
3. optional dead letter queue

Notes
------------------------------------------------------

1. intended for production? **yes**/**no**

    * yes as in should be simple enough to operate
    * yes, if you can expect a bit of bug here and there
    * yes, as this implementation should be fast enough for most case
    * no, as in no security mechanism provided

   Also becoming the reason this repo won't provide pre-compiled binary.
   If you install it and put it to production (not recommended), I hope you know what you are doing.

2. support priority? **NO**

    * separate queue for each priority (possible, but not now)

3. shared buffers? **no need** (for now)

    * the size should be small enough to fit even in memory of a relatively small machine, and recovery is based on log
    * currently hardcoded at 50K, for simplicity. Meaning, at most you may have 50K outstanding tasks

4. topic? **NO**

    * it will complicate the codebase (for now) as it is intended primarily for learning, and actually, you can run 1 instance as 1 topic :D

5. config file? later

    * this should be easy if the core logic is done (for now, change the code directly)

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

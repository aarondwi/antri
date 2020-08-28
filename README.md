# antri
toy implementation of in-memory, asynchronous task queue. Primarily intended for learning

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

This will return the key of the created task (for example, `LJKftTj3N0gOGaJO`). The key is a 16-byte alphanumeric. This task is also sync to disk to ensure durability (currently with O_SYNC flag). Note the `secondsfromnow` part, it is to set at what time should the task starts to be visible (for retrieval by workers)

From your workers, you can retrieve the task with

```shell
curl http://127.0.0.1:8080/retrieve
```

This will return the json of the task, looks like below

```json
{
    "scheduledAt":1596445069,
    "key":"LJKftTj3N0gOGaJO",
    "value":"helloworld",
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
# HTTP status code 200
OK

# if failed, probably cause of timeout or non-existent key
# HTTP status code 404
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

1. snapshot, for recovery -> following redis/LSM-tree model (rolling log already implemented)
2. properly implement shutdown (over admin api too perhaps?)
3. optional cluster (raft / lock-service based)
4. optional dead letter queue

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
    * currently hardcoded to cap at 1 million, for simplicity. Meaning, at most you may have 1 million outstanding tasks (also should be really big for most case)

4. topic? **NO**

    * it will complicate the codebase (for now) as it is intended primarily for learning, and actually, you can run 1 instance as 1 topic :D

5. config file? later

    * this should be easy if the core logic is done (for now, change the code directly)

6. stats? later

    * I am also still thinking what stats should be provided :D, and probably gonna be using prometheus

Possible optimization
------------------------------------------------------------------------

1. batch file write using fsync (based on time and/or number)

    * currently using os.O_SYNC flag (so synced on every write), but on windows (as of august 2020), the official golang compiler does not map O_SYNC to windows equivalent, so it is currently unsafe on windows.

2. reduce lock contention. Wwhat has come to mind:

    * change pq and sl to use lock-free data structure
    * try simple array as internal DS + a b-tree/skiplist for index order, to reduce the number of swapping
    * Use multiple internal queue (but because pull-model, comes problem to decide how to get from those)

3. use batching on internal api, e.g. on re-put dead task to wal and queue

4. allow all API to use batching, to reduce network round trip

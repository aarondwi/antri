# antri
A fast, durable, task-scheduling queue. This is intended as an exercise, but hopefully can be made into a production ready one.

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

0. Targeted to work both on windows and linux only
1. using grpc, easy to create client
2. batch add/get/commit
3. at least once delivery (with task timeout)
4. task scheduling/visibility, e.g. how many seconds from commit before the task can be retrieved
5. durability (via wal, and periodic snapshotting for faster recovery)

Notes
------------------------------------------------------

1. **NOT** production ready
2. There are no supports for topic/subscription. This implementation focus on the internal of each queue. Topic/subscription can easily be implemented on top of it. And anyway, priority and/or scheduled task (imo) don't match with concept of subscription (multiple consumer group).
3. Following above, multi-tenancy also not implemented (yet). This one gonna be useful for smaller usage which aims to maximize resource usage.

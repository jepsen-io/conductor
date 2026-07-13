# Jepsen Conductor Test

A test suite for [Orkes Conductor](https://orkes.io/content).

## Quickstart

You'll need a [Jepsen
environment](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment).
We recommend at least eight nodes: one for Postgres, one for Redis, and two each
for the server, worker, and standard worker. This test uses a lot of disk
space---try 48 GB for each DB node, and 128 GB for the control node.

You will also need a directory for the Orkes Conductor build you'd like to
test. This directory should have a subdirectory, `lib`, which has two files:
`server.jar` and `worker.jar`, for the Orkes server and standard worker,
respectively. You'll pass this directory as `--version <dir>` when running the
test. For instance, you might have:

```
versions/5.3.2/lib/server.jar
versions/5.3.2/lib/worker.jar
```

If you've set up an AWS Marketplace Cloudformation cluster, you can run
something like:

```sh
lein run test \
  --username admin \
  --nodes-file ~/nodes \
  --version versions/5.4.0 \
  --db-type postgres \
  --nemesis kill-server \
  --nemesis-interval 20 \
  --target-error-types incomplete \
  --time-limit 120 \
  --test-count 20 \
```

This will run a test on version 5.4.0, using Postgres as the main DB, killing
the server nodes roughly every 20 seconds, and hunting specifically for an
`incomplete` error, which occurs when a workflow is not complete at the end of
the test.

## What's Here?

We install Postgres as a DB on the first node, Redis as a queue on the second,
and divide remaining nodes evenly between Conductor servers, standard workers,
and workers. Workers are built from the `worker/` project.

A brief overview of namespaces:

- [`jepsen.conductor.cli`](src/jepsen/conductor/cli.clj) is the main entry
  point: it parses CLI options and builds and runs tests.`
- [`jepsen.conductor.base`](src/jepsen/conductor/base.clj) provides a few
  common support functions.
- [`jepsen.conductor.core`](src/jepsen/conductor/core.clj) overrides Jepsen's
  internals to let workers submit events directly to the history.
- [`jepsen.conductor.client`](src/jepsen/conductor/client.clj) wraps the
  Conductor Java SDK.
- [`jepsen.conductor.db`](src/jepsen/conductor/db.clj) sets up and tears down
  the whole cluster
- [`jepsen.conductor.db.server`](src/jepsen/conductor/db/server.clj) sets up
  and tears down server nodes
- [`jepsen.conductor.db.std-worker`](src/jepsen/conductor/db/std_worker.clj)
  sets up and tears down the standard worker that ships with Orkes Conductor.
- [`jepsen.conductor.db.worker`](src/jepsen/conductor/db/worker.clj) sets up
  and tears down our custom worker.
- [`jepsen.conductor.nemesis`](src/jepsen/conductor/nemesis.clj) injects faults
  within and between various roles.
- [`jepsen.conductor.workload`](src/jepsen/conductor/workload.clj) is the main
  workload, and defines how operations are applied to the client.
- [`jepsen.conductor.generator`](src/jepsen/conductor/generator.clj) generates
  operations, including adaptive rate limiting and tracking in-progress and
  completed workflows.
- [`jepsen.conductor.checker.workflow`](src/jepsen/conductor/checker/workflow.clj)
  checks histories for correctness.
- [`jepsen.conductor.checker.plots`](src/jepsen/conductor/checker/plots.clj)
  renders plots of queue size and startWorkflow requests; this is helpful for
  tuning the generator and seeing whether the cluster was overwhelmed.

For an explanation of each kind of error, see
[`jepsen.conductor.checker.workflow/checker`](src/jepsen/conductor/checker/workflow.clj).

## Notes

Conductor seems *much* slower to start and recover on the EC2 VMs I've been
testing with. The default nemesis interval of 5 seconds is almost certainly too
rapid; almost every operation will fail, preventing the test from doing
anything interesting. I suggest 20 seconds or more.

## License

Copyright © Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
https://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.

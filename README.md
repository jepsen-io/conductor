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

If you've set up an AWS Marketplace Cloudformation cluster, you can run:

```
lein run test --username admin --nodes-file ~/nodes --time-limit 5 --version
versions/5.3.2
```

## What's Here?

We install Postgres as a DB on the first node, Redis as a queue on the second,
and divide remaining nodes evenly between Conductor servers and workers.
Workers are built from the `worker/` project.

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

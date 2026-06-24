# Jepsen Conductor Test

A test suite for [Orkes Conductor](https://orkes.io/content).

## What's Here?

We install Postgres as a DB on the first node, Redis as a queue on the second,
and divide remaining nodes evenly between Conductor servers and workers.
Workers are built from the `worker/` project.

## Usage

You'll need a [Jepsen environment](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment).

```
lein run test-all
```

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

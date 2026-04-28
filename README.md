mindroom-nio
============

[![Build Status](https://img.shields.io/github/actions/workflow/status/mindroom-ai/mindroom-nio/tests.yml?branch=main&style=flat-square)](https://github.com/mindroom-ai/mindroom-nio/actions)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mindroom-nio?style=flat-square)](https://pypi.org/project/mindroom-nio/)
[![codecov](https://img.shields.io/codecov/c/github/mindroom-ai/mindroom-nio/main.svg?style=flat-square)](https://codecov.io/gh/mindroom-ai/mindroom-nio)
[![license](https://img.shields.io/badge/license-ISC-blue.svg?style=flat-square)](https://github.com/mindroom-ai/mindroom-nio/blob/main/LICENSE.md)
[![Documentation Status](https://readthedocs.org/projects/matrix-nio/badge/?version=latest&style=flat-square)](https://matrix-nio.readthedocs.io/en/latest/?badge=latest)
[![#nio](https://img.shields.io/badge/matrix-%23nio:matrix.org-blue.svg?style=flat-square)](https://matrix.to/#/!JiiOHXrIUCtcOJsZCa:matrix.org?via=matrix.org&via=maunium.net&via=t2l.io)

This fork exists primarily because I need vodozemac-backed E2EE support for
[MindRoom](https://github.com/mindroom-ai/mindroom), which relies heavily on
nio. libolm is officially deprecated, and the `python-olm` wheels currently stop
at CPython 3.12, making the old dependency path a poor fit for newer Python
versions. This fork keeps that support, plus a small set of other useful
pending upstream PRs, available in a published package while upstream activity
is quiet. I would be happy to see the upstream project become active again and
reduce fork-specific maintenance over time.

mindroom-nio is a fork of nio, a multilayered [Matrix](https://matrix.org/)
client library. The distribution name is `mindroom-nio`; the Python import name
remains `nio`. The underlying base layer doesn't do any network IO on its own,
but on top of that is a full-fledged batteries-included asyncio layer using
[aiohttp](https://github.com/aio-libs/aiohttp/). File IO is only done if you
enable end-to-end encryption (E2EE).

Documentation
-------------

The full API documentation for nio can be found at
[https://matrix-nio.readthedocs.io](https://matrix-nio.readthedocs.io/en/latest/#api-documentation)

Features
--------

nio has most of the features you'd expect in a Matrix library, but it's still a work in progress.

- ✅ transparent end-to-end encryption (EE2E)
- ✅ encrypted file uploads & downloads
- ✅ space parents/children
- ✅ manual and emoji verification
- ✅ custom [authentication types](https://matrix.org/docs/spec/client_server/r0.6.0#id183)
- ✅ threading support
- ✅ well-integrated type system
- ✅ knocking, kick, ban and unban
- ✅ typing notifications
- ✅ message redaction
- ✅ token based login
- ✅ user registration
- ✅ read receipts
- ✅ live syncing
- ✅ `m.reaction`s
- ✅ `m.tag`s
- ❌ cross-signing support
- ❌ server-side key backups (room key backup, "Secure Backup")
- ❌ user deactivation ([#112](https://github.com/matrix-nio/matrix-nio/issues/112))
- ❌ in-room emoji verification

Installation
------------

To install mindroom-nio, simply use pip:

```bash
$ pip install mindroom-nio
```

Note that this installs mindroom-nio without end-to-end encryption support. The
e2ee enabled version can be installed using pip:

```bash
$ pip install "mindroom-nio[e2e]"
```

Additionally, a docker image with the e2ee enabled version of nio is provided in
the `docker/` directory.

Examples
--------

For examples of how to use nio, and how others are using it,
[read the docs](https://matrix-nio.readthedocs.io/en/latest/examples.html)

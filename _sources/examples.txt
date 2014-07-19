
Examples
============

.. highlight:: lua
   :linenothreshold: 5


Pub/Sub
-------

This example shows how to to Publish/Subscribe with TurboRedis.

.. literalinclude:: ../examples/pubsub.lua
    :lines: 2-


GitHub API Caching
------------------

This example is a simple app that retrieves the repository list of
a GitHub user and caches the responses from the GitHub API in Redis for a
limited time.

.. literalinclude:: ../examples/github_repolist.lua


Command Line Redis Client
-------------------------

This example implments a super-simple command line client
for doing Redis commands.

.. literalinclude:: ../examples/rediscmd.lua

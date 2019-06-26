================================================================
 LiveCheck: End-to-end test for production/staging.
================================================================

What is the problem with unit tests?
What is difficult about maintaining integration tests?
Why testing in production makes sense.
Bucket testing, slow deploy of new tests will give confidence of a release.
A staging environment is still desirable.

Enables you to:

    - track requests as they travel through your micro service architecture.
    - define contracts that should be met at every step

all by writing a class that looks like a regular unit test.

This is a passive observer, so will be able to detect and complain when
subsystems are down. Tests are executed based on probability, so you can
run tests for every requests, or for just 30%, 50%, or even 0.1% of your
requests.

This is not just for micro service architectures, it's for any asynchronous
system.  A monolith sending celery tasks is a good example, you could
track and babysit at every step of a work flow to make sure things
progress as they should.

Every stage of your production pipeline could be tested for
things such as "did the account debt exceed a threshold after this change",
or "did the account earn a lot of credits after this change".

This means LiveCheck can be used to monitor and alert on anomalies happening
in your product, as well as for testing general reliability and the consistency
of your distributed system.

Every LiveCheck test case is a stream processor, and so can use tables to store data.


Tutorial
========

LiveCheck Example.

1) First start an instance of the stock ordering system in a new terminal:

.. sourcecode:: console

    $ python examples/livecheck.py worker -l info

2) Then in a new terminal, start a LiveCheck instance for this app

.. sourcecode:: console

    $ python examples/livecheck.py livecheck -l info

3) Then visit ``http://localhost:6066/order/init/sell/`` in your browser.

    Alternatively you can use the ``post_order`` command:

    .. sourcecode:: console

        $ python examples/livecheck.py post_order --side=sell

The probability of a test execution happening is 50%
so you have to do this at least twice to see activity happening
in the LiveCheck instance terminal.

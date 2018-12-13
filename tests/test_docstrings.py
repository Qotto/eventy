# coding: utf-8
# Copyright (c) Qotto, 2018

import doctest
import unittest


def load_tests(loader, tests, pattern):
    tests.addTests(doctest.DocTestSuite('eventy.event.base'))
    tests.addTests(doctest.DocTestSuite('eventy.event.generic'))
    tests.addTests(doctest.DocTestSuite('eventy.serializer.base'))
    tests.addTests(doctest.DocTestSuite('eventy.serializer.dummy'))
    tests.addTests(doctest.DocTestSuite('eventy.serializer.avro'))
    tests.addTests(doctest.DocTestSuite('eventy.store.base'))
    tests.addTests(doctest.DocTestSuite('eventy.store.kafka'))
    tests.addTests(doctest.DocTestSuite('eventy.store.memory'))
    tests.addTests(doctest.DocTestSuite('eventy.utils'))
    return tests

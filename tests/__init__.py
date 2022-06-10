import unittest
from os import getenv, environ
try:
    from atlasapi.atlas import Atlas
except NameError:
    from atlas import Atlas


class BaseTests(unittest.TestCase):
    def setUp(self):
        self.USER = getenv('ATLAS_USER', None)
        self.API_KEY = getenv('ATLAS_KEY', None)
        self.GROUP_ID = getenv('ATLAS_GROUP', None)


        if not self.USER or not self.API_KEY or not self.GROUP_ID:
            raise EnvironmentError('In order to run this smoke test you need ATLAS_USER, AND ATLAS_KEY env variables'
                                   'your env variables are {}'.format(environ.__str__()))
        self.a = Atlas(self.USER, self.API_KEY, self.GROUP_ID)

    # executed after each test

    def tearDown(self):
        pass
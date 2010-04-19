from gorg_site.tests import *

class TestDirectorController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='director', action='index'))
        # Test response...

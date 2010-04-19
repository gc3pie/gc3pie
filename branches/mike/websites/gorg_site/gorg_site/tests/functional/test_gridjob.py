from gorg_site.tests import *

class TestGridjobController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='gridjob', action='index'))
        # Test response...

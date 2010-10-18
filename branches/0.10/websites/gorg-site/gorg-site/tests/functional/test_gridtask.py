from gorg_site.tests import *

class TestGridtaskController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='gridtask', action='index'))
        # Test response...

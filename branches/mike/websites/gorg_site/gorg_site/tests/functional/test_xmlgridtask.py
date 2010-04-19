from gorg_site.tests import *

class TestXmlgridtaskController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='xmlgridtask', action='index'))
        # Test response...

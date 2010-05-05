from gorg_site.tests import *

class TestXmlgridjobController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='xmlgridjob', action='index'))
        # Test response...

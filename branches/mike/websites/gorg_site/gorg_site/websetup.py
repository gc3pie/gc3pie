"""Setup the gorg_site application"""
import logging

from gorg_site.config.environment import load_environment

log = logging.getLogger(__name__)

def setup_app(command, conf, vars):
    """Place any commands to setup gorg_site here"""
    load_environment(conf.global_conf, conf.local_conf)

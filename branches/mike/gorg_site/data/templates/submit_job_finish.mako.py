from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1267176602.4468069
_template_filename='/home/mmonroe/apps/gorg/gorg_site/gorg_site/templates/submit_job_finish.mako'
_template_uri='/submit_job_finish.mako'
_template_cache=cache.Cache(__name__, _modified_time)
_source_encoding='utf-8'
from webhelpers.html import escape
_exports = []


def render_body(context,**pageargs):
    context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        c = context.get('c', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 1
        __M_writer(u'<html>\n\n<head>\n<title>Result of upload </title>\n\n')
        # SOURCE LINE 6
        __M_writer(escape(c.mess))
        __M_writer(u'\n\n</html>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()



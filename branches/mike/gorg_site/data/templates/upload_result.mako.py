from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1267032902.121933
_template_filename='/home/mmonroe/apps/gorg/gorg_site/gorg_site/templates/upload_result.mako'
_template_uri='/upload_result.mako'
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
        __M_writer(u'<html>\n  <body>\n    ')
        # SOURCE LINE 3
        __M_writer(escape(c.mess))
        __M_writer(u'\n  </body>\n</html>\n\n\n')
        return ''
    finally:
        context.caller_stack._pop_frame()



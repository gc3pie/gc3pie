from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1268312751.0144939
_template_filename='/home/mmonroe/apps/gorg/gorg_site/gorg_site/templates/submit_job_detail.mako'
_template_uri='/submit_job_detail.mako'
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
        __M_writer(u'<html>\n\n<head>\n<title>Result of upload </title>\n</head>\n\n<body>\n<p>Title: ')
        # SOURCE LINE 8
        __M_writer(escape(c.a_job.title))
        __M_writer(u'</p>\n<p>Author: ')
        # SOURCE LINE 9
        __M_writer(escape(c.a_job.author))
        __M_writer(u'</p>\n<p>Attachments: </p>\n<table>\n')
        # SOURCE LINE 12
        for attachment in c.a_job['_attachments']:
            # SOURCE LINE 13
            __M_writer(u'                <tr>\n                <td>')
            # SOURCE LINE 14
            __M_writer(escape(attachment))
            __M_writer(u'</td>\n                <td><a href="display_job_attachment?jobid=')
            # SOURCE LINE 15
            __M_writer(escape(c.a_job.id))
            __M_writer(u';attachment=')
            __M_writer(escape(attachment))
            __M_writer(u'">Download</a></td>\n                </tr>\n')
        # SOURCE LINE 18
        __M_writer(u'\n</table>\n\n</body>\n</html>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()



from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1268309644.2872269
_template_filename='/home/mmonroe/apps/gorg/gorg_site/gorg_site/templates/query_task_finish.mako'
_template_uri='/query_task_finish.mako'
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
        __M_writer(u'<html>\n<body>\n<table>\n')
        # SOURCE LINE 4
        for job in c.job_list:
            # SOURCE LINE 5
            __M_writer(u'\t\t<tr>\n\t\t<td><a href="/gridjob/query_job?jobid=')
            # SOURCE LINE 6
            __M_writer(escape(job.id))
            __M_writer(u'">')
            __M_writer(escape(job.id))
            __M_writer(u'</a></td>\n\t\t<td>')
            # SOURCE LINE 7
            __M_writer(escape(job.title))
            __M_writer(u'</td>\n\t\t<td>')
            # SOURCE LINE 8
            __M_writer(escape(job.user_params['restart_number']))
            __M_writer(u'</td>\n\t\t<td>')
            # SOURCE LINE 9
            __M_writer(escape(job.status))
            __M_writer(u'</td>\n\t\t</tr>\n')
        # SOURCE LINE 12
        __M_writer(u'\n</table>\n</body>\n</html>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()



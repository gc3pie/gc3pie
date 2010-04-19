from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1267029520.675993
_template_filename='/home/mmonroe/apps/gorg/gorg_site/gorg_site/templates/submit_form.mako'
_template_uri='/submit_form.mako'
_template_cache=cache.Cache(__name__, _modified_time)
_source_encoding='utf-8'
from webhelpers.html import escape
_exports = []


def render_body(context,**pageargs):
    context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        h = context.get('h', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 1
        __M_writer(u'\n')
        # SOURCE LINE 2
        __M_writer(escape(h.form(h.url_for(action='upload'), multipart=True)))
        __M_writer(u'\nAuthor:\t\t ')
        # SOURCE LINE 3
        __M_writer(escape(h.text('author')))
        __M_writer(u'\nUpload file:      ')
        # SOURCE LINE 4
        __M_writer(escape(h.file('myfile')))
        __M_writer(u' <br />\nFile description: ')
        # SOURCE LINE 5
        __M_writer(escape(h.text('title')))
        __M_writer(u' <br />\n                  ')
        # SOURCE LINE 6
        __M_writer(escape(h.submit('aname','Submit')))
        __M_writer(u'\n')
        # SOURCE LINE 7
        __M_writer(escape(h.end_form()))
        __M_writer(u'\n')
        return ''
    finally:
        context.caller_stack._pop_frame()



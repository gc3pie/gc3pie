import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render
from gorg_site.model.gridjob import GridjobModel
from gorg_site.model.gridtask import GridtaskModel
import webhelpers.paginate as paginate

from gorg_site.lib.mydb import Mydb

log = logging.getLogger(__name__)

class GridtaskController(BaseController):
    
    def view_user_task_overview(self):
        # id is the author name
        author=session['author']
        session.save()
        if author is None:
            abort(404)            
        # Fill the template info
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        c.content = "This is page %s"%author
        # Get the info to display        
        db=Mydb().cdb()
        # Here we query the database and get the number of records for that
        # author in the given state
        counts = dict()
        for a_status in GridtaskModel().POSSIBLE_STATUS:
            view = GridtaskModel().view(db, 'by_author_task_status')
            # When a status does not match, a None is returned.
            # We therefore have to convert the None into a 0
            a_row = view[[author, a_status]].rows
            if a_row:
                counts[a_status] = view[[author,   a_status]].rows[0]['value']
            else:
                counts[a_status] = 0
        c.author_job_status_counts = counts
        if c.author_job_status_counts is None:
            abort(404)            
        return render('/derived/user_task_overview.html')

    def view_user_tasks(self, id=None):
        """A list of the tasks a given author in the given state is generated."""
        author = session['author']
        status = id
        # Fill the template info
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        c.content = "This is page %s"%author
        db=Mydb().cdb()
        view = GridtaskModel().view(db, 'by_status', key=status)
        records = list()
        for a_task in view:
            records.append(a_task)
        c.paginator = paginate.Page(
            records,
            page=int(request.params.get('page', 1)),
            items_per_page = 10,
            item_count=len(records)
            )            
        return render('/derived/user_tasks.html')

    def view_task(self, id=None):
        """Post / users: Query an existing task in the database."""
        if id is None:
            abort(404)
        # Fill the template info in
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        c.content = "This is page."
        db=Mydb().cdb()
        c.a_task = GridtaskModel().load(db,id)
        records = c.a_task.get_jobs(db)
        c.paginator = paginate.Page(
            records,
            page=int(request.params.get('page', 1)),
            items_per_page = 10,
            item_count=len(records)
            )   
        return render('/derived/view_task.html')

    def query(self):
        db=Mydb().cdb()
        task_id=request.POST['task_id']
        c.task = GridtaskModel().load(db, task_id)
        
        
        return render('/query_task_finish.mako')

    def query_form(self):
        return render('/query_task_form.mako')
    
    def query(self):
        db=Mydb().cdb()
        task_id=request.POST['task_id']
        c.task = GridtaskModel().load(db, task_id)
        c.job_list = c.task.get_jobs(db)
        return render('/query_task_finish.mako')

    def create(self):
        new_task = GridtaskModel()
        myfile = request.POST['myfile']
        title = request.POST['title']
        author = request.POST['author']
        user_type = 'GAMESS'
        new_task.create(title, title, user_type)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile, title)
        return render('/submit_task_finish.mako')

import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render
from gorg.model.gridjob import *
from gorg.model.gridtask import GridtaskModel, TaskInterface
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
        view = GridtaskModel.view_author(db, key = author)
        task_interface = TaskInterface(db)
        status_dict = dict()
        for a_status in PossibleStates:
            status_dict[a_status]=0
        for a_task in view:
            task_interface.task = a_task
            status_dict[task_interface.status_overall] +=1
        c.author_job_status_counts = status_dict
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
        view = GridtaskModel.view_author(db, key = author)
        records = list()
        for a_task in view:
            task_interface = TaskInterface(db)
            task_interface.task=a_task
            if task_interface.status_overall == status:
                records.append(task_interface)
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
        c.a_task = TaskInterface(db).load(id)
        records = c.a_task.children
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

import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to
from gorg_site.lib.base import BaseController, render
import webhelpers.paginate as paginate
from pylons.decorators import jsonify
import os

from gorg.model.gridjob import *
from gorg_site.lib.mydb import Mydb

log = logging.getLogger(__name__)

class GridjobController(BaseController):
    
    def view_user_job_overview(self):
        """An overview of all the jobs the author (id)
        has in the database is displayed as well as there status."""
        # id is the author name
        author=session['author']
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
        for a_status in PossibleStates:
            view = GridrunModel.view_author_status(db)
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
        return render('/derived/user_job_overview.html')

    def view_user_jobs(self, id=None):
        """A list of the jobs a given author in the given state is generated."""
        author = session['author']
        status = id
        # Fill the template info
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        c.content = "This is page %s"%author
        db=Mydb().cdb()
        view = GridjobModel.view_author_status(db, key=(author, status))
        records = list()
        for a_job in view:
            records.append(a_job)
        c.paginator = paginate.Page(
            records,
            page=int(request.params.get('page', 1)),
            items_per_page = 10,
            item_count=len(records)
            )            
        return render('/derived/user_jobs.html')
    
    def view_job(self, id=None):
        """Post / users: Query an existing job in the database."""
        if id is None:
            abort(404)
        # Fill the template info in
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        c.content = "This is page."
        db=Mydb().cdb()
        a_job = JobInterface(db).load(id)
        c.a_job = a_job
        return render('/derived/view_job.html')

    def view_job_attachment(self, id=None):
        """Post / users: Query an existing job in the database."""
        if id is None:
            abort(404)
        attachment = request.GET['attachment']
        db=Mydb().cdb()
        a_job = JobInterface(db).load(id)
        response.content_type = 'text/plain'
        return a_job.get_attachment(attachment)

#--------------------------- below not using now
    def submit_form(self):
        return render('/submit_job_form.mako')
    
    def create(self):
        """Post / users: Create a new job in the database."""
        # Myfile is a fieldstorage object provided by pylons
        # myfile.filename = basename of file, myfile.file = file like object
        # myfile.value = contents of file
        myfile = request.POST['myfile'] 
        title = request.POST['title']
        author = request.POST['author']
        db=Mydb().cdb()
        a_job = GridjobModel()
        a_job.author = author
        a_job.title = title
        a_job.defined_type = 'GAMESS'
        a_job.put_attachment(db, myfile.file, myfile.filename)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile.filename, title)
        return render('/submit_job_finish.mako')

    def query_job(self):
        """Post / users: Query an existing job in the database."""
        jobid = request.GET['jobid'] 
        db=Mydb().cdb()
        a_job = GridjobModel().load(db,jobid)
        c.a_job = a_job
        return render('/submit_job_detail.mako')
    
    @jsonify
    def _json_test(self):
        if request.environ['CONTENT_TYPE'] == 'application/json':
            return {'response':'I am json'}
        return render('/submit_job_form.mako')


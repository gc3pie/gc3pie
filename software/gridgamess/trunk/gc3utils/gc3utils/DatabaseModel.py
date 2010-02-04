from elixir import *

class Molecule(Entity):
    name = Field(Unicode(30),primary_key=True)
    xcoord = Field(Float)
    ycoord = Field(Float)
    zcoord = Field(Float)
    inputfiles = OneToMany('InputFile')
    
    def __repr__(self):
        return '<Molecule "%s" (%f %f %f %s)>' % (self.name, self.xcoord, self.ycoord, self.zcoord, self.inputfiles)


class InputFile(Entity):
    name = Field(Unicode(30),primary_key=True)
    status = Field(Unicode(30))
    molecule = ManyToOne('Molecule')
    resource = ManyToOne('Resource')
    
    def __repr__(self):
        return '<InputFile "%s" (%s %s %s)>' % (self.name, self.status, self.molecule, self.resource)


class Resource(Entity):
    name = Field(Unicode(30),primary_key=True)
    rtype = Field(Unicode(30))
    gb_per_host = Field(Integer)
    gamess_version = Field(Unicode(30))
    inputfiles = OneToMany('InputFile')
#    jobrun = OneToOne('JobRun',inverse='resource')
    
    def __repr__(self):
        return '<Resource "%s" (%s %d %s %s %s)>' % (self.name, self.rtype, self.gb_per_host, self.gamess_version, self.inputfiles, self.jobrun)


class JobRun(Entity):
    name = Field(Unicode(30),primary_key=True)
#    resource = OneToOne('Resource',inverse='jobrun')
    
    def __repr__(self):
        return '<JobRun "%s" (%s %s)>' % (self.name, self.resource)



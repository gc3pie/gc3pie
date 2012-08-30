'''
@Module
gplot is a python 3.x wrapper class to gnuplot 4.x
@Author
Mohammad Rahmani <mohammad [dot] rahmani [at] gmail [dot] com>
Amirkabir Uni of Tech, Teh, Ir
@License
This file is licensed under the GNU Lesser General Public License (LGPL)
@Version
alpha, Aug 27, 2010
'''
# gplot is a very simple pure Python class implementing a wrapper class to gnuplot.
# it has been tested on Windows 32 [XP, Vista, 7]
# Some functions are borrowed from Gnuplot: [http://gnuplot-py.sourceforge.net]
# Copyright (C) 1998-2003 Michael Haggerty <mhagger@alum.mit.edu>
# Gnuplot is a powerful package from Michael Haggerty to use gnuplot under python.
# It requires numpy.
#
#
# Installation:
#
# Just copy paste this module in your Python path, \Lib\site-packages is a recommended
# folder under Python installation folder
#
# System requirements
# python 3 and later
#
# Linux/Unix
# Change pgnuplot.exe to gnuplot.exe in __init__ module.




from os import popen
class Gnuplot():
  '''
    gnuplot interface. Pipeline is opened to gnuplot. Then commands are written to file and file is loaded through pipe. 
    command is something like plot '3 4 2 3' '3 4 5 6', so incorporates boht plotting command and data. 
  '''
  def __init__(self,options='',persist=None,):
    # self.__g is a pipe to gnuplot
    if persist is not None:
      self.__g = popen('gnuplot -persist', 'w')
    else:
      self.__g = popen('gnuplot', 'w')
    self.__file_counter=0
    self.__options=options
    self.__newopt=False
    self.plotCmd = 'plot'

  def __write2gnuplot(self, command):
    """write commands into gnuplot program"""
    self.__g.write(command + '\n')
    print('writing: ' + command + '\n')
 #   self.__g.write('\n')
    self.__g.flush()

  def script(self,cmd):
    """Direct commands in gnuplot"""
    pcmd=self.__processcmd(cmd,'gnuplot-script')
    self.__write2gnuplot(pcmd)
  def close(self):
    """closes gnuplot"""
    self.__g.close()
  def reset(self):
    """
    The reset command causes all graph-related options
    that can be set with the set command to take on their default values
    exceptions are 'set term' 'set output', ...
    See gnuplot docs for further help
    """
    self.__options='' # Clear all options
    self.__write2gnuplot('reset')
  def refresh(self):
    """
    write the latest options to gnuplot
    and refresh the last graph
    """
    if self.__newopt:
      self.__write2gnuplot(self.__options)
      self.__newopt=False
    self.__write2gnuplot('refresh')
  def fplot(self,fun,limits,linespec='',N=25):
    """plot function f in range of limits with N points"""

    #Check input arguments
    n=len(limits)
    if n==2:
      xmin, xmax=limits
      ymin=ymax=None
      if (xmin>xmax): raise Exception("fplot error: wrong limits data, -101")
    elif n==4:
      xmin, xmax, ymin, ymax=limits
      if (xmin>xmax) or (ymin>ymax): raise Exception("fplot errors: wrong limits data,-101")

    else:
      raise Exception("fplot error: wrong limits, use limits=[xmin,xmax]\n or limits=[xmin, xmax, ymin, ymax], -101")

    dx=(xmax-xmin)/float(N-1)
    x=[float(i)*dx+xmin for i in range(N) ]
    try: #Check to see how many output function f returns
      tmp=fun(x[0])
      nvalue=len(tmp)
      y=[[] for i in range(nvalue)]
      for j in range(N):
        tmp=fun(x[j])
        for i in range(nvalue):
          y[i].append(tmp[i])
    except: #When function f returns single value
      y=[fun(x[i]) for i in range(N)]
    # now plot data
    self.plot(x,y,linespec)

  def plot(self, x,*args):
    """
    plots x,y pairs using gnuplot
    if y was not passed, only x will be drawn!
    if y is a matrix, each row is drawn againest x
    """
    #dataset contains sets of data each containing:
    #[x, y, linespec, ArrType, nr, nc]
    dataset=self.__processArgs(x,*args)
    # open a file to write plot data
    self.__file_counter += 1
    filename = '.tmp_%04d' % self.__file_counter
    f = open(filename, 'w')
    #write gnuplot options, if there is any new option set!
    if self.__newopt:
      self.__write2gnuplot(self.__options)
      self.__newopt=False
    #Write plot commands
    f.write(self.plotCmd + ' \\\n')
    plotcommand=[]
    for ds in dataset:
      pltcmd=self.__genpltcmd(ds)
      plotcommand.append(pltcmd)

    plotcommand = ',\\\n'.join(plotcommand) + '\n'
    f.write(plotcommand)
    status=self.__writedata(dataset,f)
    f.close()
    self.__write2gnuplot('load "{0:s}"'.format(filename))

  def __genpltcmd(self,ds):
    '''generate the plot command string for each dataset'''
    #ds=[x, y, linespec, ArrType, nr, nc]

    def processlinespec(nr, linespec):
      cmd=[]
      if type(linespec)== tuple:
        nt=len(linespec)
        if nt != nr:
          raise Exception("plot error: wrong number of linespec for matrix plot,-102")
        cmd=['"-" %s' % linespec[i] for i in range(nt)]
      elif type(linespec)==str:
        cmd=['"-" %s' % linespec]*nr
      else: # linespec is None
        cmd=['"-"']*nr
      pcmd=','.join(cmd)
      return pcmd

    linespec, ArrType, nr =ds[2:5]
    if ArrType=='vector':
      if linespec is not None:
        plotcmd ='"-" %s' % linespec
      else:
        plotcmd='"-"'
      return plotcmd
    else: #ArrType is matrix
      plotcmd=processlinespec(nr, linespec)
      return plotcmd
  @property
  def options(self):
    """return gnuplot options"""
    return self.__options
  @options.setter
  def options(self,value):
    """
    customize the gnuplot program using set/unset commands
    """
    opt=self.__options
    if opt:
      self.__options=opt+';'+ self.__processcmd(value,'gnuplot-options')
    else:
      self.__options=self.__processcmd(value,'gnuplot-options')
    self.__newopt=True

  @staticmethod
  def __processcmd(cmd,cmdType):
    """process text commands to be sent to gnuplot program"""
    # Remove extra spaces, remove EOLs.
    pcmd=";".join([s.strip() for s in cmd.split("\n") if s.strip()])
    # Merge commands spanned over multi-lines
    pcmd=pcmd.replace('\\;','')
    if cmdType=='gnuplot-options':
      #Check the validity of command
      #Each command has to be startted by a "set" or "unset" keyword
      for k in pcmd.split(';'):
        if k.split()[0].strip() not in ['set','unset']:
          raise Exception("Wrong option commands, use set/unset for gnuplot options,-105")
          return None
    # else it is a gnu-script
    return pcmd

  def xlabel(self, txt=None, color=None, font=None):
    """Set the plot's xlabel."""
    self.__set_label('xlabel', txt, color=color, font=font)

  def ylabel(self, txt=None, color=None, font=None):
    """Set the plot's ylabel."""
    self.__set_label('ylabel', txt, color=color, font=font)

  def zlabel(self, txt=None, color=None, font=None):
    """Set the plot's zlabel."""
    self.__set_label('zlabel', txt, color=color, font=font)
  def title(self, txt=None, color=None, font=None):
    """Set the plot's title."""
    self.__set_label('title', txt, color=color, font=font)
  def __set_label(self, option, txt=None, color=None, font=None):
    """Set or clear a label option, which can include an color or font."""
    cmd = ['set', option]
    if txt is not None:
      cmd.append('"%s"' % (txt,))
      if color is not None:
        if color[0]=="#":
          cmd.append("tc rgb '%s'" % color)
        else:
          cmd.append("tc rgbcolor '%s'" % color)

      if font is not None:
        cmd.append("font '%s'" % (font,))
    cmd=' '.join(cmd)
    self.__write2gnuplot(cmd)

  def __processArgs(self,x,*args):
    """Analyzes the plot input arguments and put them in a list of list each containing a set of [x,y,linespec]"""
    arrType, nr, nc=self.__checkarray(x)
    if arrType not in ['vector','matrix']:
      raise Exception("wrong data type, first argument shall be of type list,-103")

    dataset=[]
    n=len(args)
    if n==0:
      dataset.append([x,None,None,arrType,nr,nc])
      return dataset
    elif n==1:
      if type(args[0]) in (str,tuple):
        dataset.append([x,None,args[0],arrType,nr,nc])
        return dataset
      elif type(args[0])==list:
        arrType, nr, nc=self.__checkarray(args[0])
        dataset.append( [x,args[0],None,arrType,nr,nc])
        return dataset
    # When n>1, we have other pairs of data
    i=0
    arrType, nr, nc=self.__checkarray(args[i])
    if arrType not in ['vector','matrix']:
      raise Exception("wrong data type, x,y pairs shall be of type list,-103")
    dset=[x,args[i],None,arrType,nr,nc]
    i=i+1
    if type(args[i])in (str,tuple):
      dset[2]=args[i]
      i=i+1
    dataset.append(dset)
    dset=[]
    while i+1<n:
      if not (type(args[i])==type(args[i+1])==list):
        raise Exception("wrong data type, x,y pairs shall be of type list,-103")
      else:
        arrType, nr, nc=self.__checkarray(args[i])
        dset=[args[i],args[i+1],None,arrType,nr,nc]
        i=i+2
      if i>=n: #there is no other data
        dataset.append(dset)
        break
      if type(args[i])in (str,tuple):
        dset[2]=args[i]
        i=i+1
      dataset.append(dset)
    # if statement will be executed while there is exist data
    if i<n and i+1==n: # it needs at least two other in args
      raise Exception("wrong data type, x,y,s or x,y pairs shall be provided,-104")
    return dataset

  def __writedata(self,dataset,f):
    """Write all data into gnuplot script file"""
    for dset in dataset:
      x, y=dset[0], dset[1]
      if y is not None:
        try:
          nr,nc=len(y), len(y[0])
          self.__writematrix(x,y,nr,nc,f)
        except:
          nc=len(y)
          self.__writevector(x,y,nc,f)
      else: # There is no y data
        try: # x is matrix or not
          nr, nc=len(x), len(x[0])
          self.__writematrix(x,None,nr,nc,f)
        except: # x is a vector
          nc=len(x)
          self.__writevector(x,None,nc,f)
    return 0
  def __writematrix(self,x,y,nr,nc,f):
    """write a matrix of data into gnuplot script file"""
    if y is not None:
      for i in range(nr):
        for j in range(nc):
          f.write('{0:g} {1:g}\n'.format(x[j],y[i][j]))
        f.write('e\n')
    else:
      for i in range(nr): #single argumnt, but x is a matrix
        for j in range(nc):
          f.write('{0:d} {1:g}\n'.format(j,x[i][j]))
        f.write('e\n')
    return 0
  def __writevector(self,x,y,nc,f):
    """write a vaector of data into gnuplot script file"""
    if y is not None:
      for j in range(nc):
        f.write('{0:g} {1:g}\n'.format(x[j],y[j]))
      f.write('e\n')
    else: #single argument
      for j in range(nc):
        f.write('{0:d} {1:g}\n'.format(j, x[j]))
      f.write('e\n')
    return 0
  def __checkarray(self, arr):
    """ checks arr for matrix and vector, returns type and size"""
    if type(arr) != list:
      return type(arr), None, None
    try:
      nr, nc=len(arr), len(arr[0])
      return 'matrix', nr, nc
    except:
      nr=len(arr)
      return 'vector', nr, None

  def splot(self,x,y=None,z=None,linespec=''):
    """
    Draw 3D plot using gnuplot command: splot
    splot first write data into a text file in
    form of set of x y z and then plot
    """
    # open a file to write plot data
    self.__file_counter += 1
    filename = '.tmp_%04d' % self.__file_counter
    f = open(filename, 'w')
    #write gnuplot options, if there is any new option set!
    if self.__newopt:
      self.__write2gnuplot(self.__options)
      self.__newopt=False
    #Write plot commands
    f.write('splot "-" %s \n' % linespec)
    f.write('#data x y z \n')
    for j in range(len(z[0])):
      for i in range(len(z)):
        f.write("%g\t%g\t%g\n" %( x[i][j], y[i][j], z[i][j] ) )
      f.write('\n')
    f.write('e\n')
    f.close()
    # splot data in gnuplot
    self.__write2gnuplot('load "{0:s}"'.format(filename))


  def fsplot(self, f, x, y, linespec='', nx=25, ny=25):
    """
    3D plot of a function over a rectangular domain of x-y.
    f is a function of x, y, e.g. z=f(x,y)
    x, y are vector.
    """
    X, Y = self.meshgrid(x, y, nx, ny)
    m, n =len(X), len(X[0])
    Z=[]
    for i in range(m):
      row=[]
      for j in range(n):
        row.append( f(X[i][j],Y[i][j]) )
      Z.append(row)
    self.splot(X,Y,Z,linespec)


  @staticmethod
  def meshgrid(x,y,nx=25,ny=25):
    """
    generate mesh grid over a rectangular domain of [xmin xmax, ymin, max]
    x, y are vector (list) in form of [start, step, stop] or [start, stop]
    when stop has not been given, nx and ny are used to calculate steps and generate
    nx and ny data points respectively.
    nx and ny will be ignored when step is given.
    X and Y are matrix (list of list) each of size [nx by ny] contains the grid data.
    The coordinates of point (i,j) is [X(i,j), Y(i,j)]
    """
    # Example
    # X, Y= gp.meshgrid([0,1,3],[5,1,8])
    # X
    # [[0.0, 1.0, 2.0, 3.0],
    # [0.0, 1.0, 2.0, 3.0],
    # [0.0, 1.0, 2.0, 3.0],
    # [0.0, 1.0, 2.0, 3.0]]
    #
    #Y
    #[[5.0, 5.0, 5.0, 5.0],
    # [6.0, 6.0, 6.0, 6.0],
    # [7.0, 7.0, 7.0, 7.0],
    # [8.0, 8.0, 8.0, 8.0]]

    def grid(v,nv=25):
      """
      generate a linear spaced vector for n points or with specified step
      v must be a list, if len(v) is 2 then a linear spaced vector with length
      nv will be created!
      if len(v) is 3 then start, step, stop=v and a linearspaced vector from strat
      with step value will be created
      """
      n=len(v)
      if n==3:
        start,step,stop=v
        dv=step
        nv=int( (stop-start)/step )+ 1
      elif n==2:
        start,stop=v
        dv=(stop-start)/(nv-1.0)
      else:
        raise Exception('meshgrid error: wrong input data')
      vv=[float(i)*dv+start for i in range(nv) ]
      #vv is the linespaced vector, nv is the len of vector
      return vv, nv


    xv, nx = grid(x, nx)
    yv, ny = grid(y, ny)
    X=[xv for i in range(ny)]
    Y=[[yv[i]]*nx for i in range(ny)]
    return X, Y

  @staticmethod
  def linspace(a, b, n=100):
    """
    returns a linearly spaced vector with n points in [a, b]
    if n is omitted, 100 points will be considered
    """
    dx = (b - a) / float(n - 1)
    return [a + i * dx for i in range(n)]



# ...............................................................................
# Demo
# The following examples demonstrate several capabilities of gplot
#
# ...............................................................................
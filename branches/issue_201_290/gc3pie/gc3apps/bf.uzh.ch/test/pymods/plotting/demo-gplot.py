#!/usr/bin/env python3 

print('hello')
'''
@Module
demo-gplot is a demo module and demonstrates the gplot library capabilities
for 2D/3D plotting using gnuplot.
@Author
Mohammad Rahmani <mohammad [dot] rahmani [at] gmail [dot] com>
Amirkabir Uni of Tech, Teh, Ir
@License
This file is licensed under the GNU Lesser General Public License (LGPL)
@Version
alpha, Aug 27, 2010
'''

import gplot

#...............................................................................
# Example 1: A very basic example
#...............................................................................

#Create a new instance of Gnuplot class
gp=gplot.Gnuplot()

# Input data
x=[-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8]
y=[66,51,38,27,18,11,6,3,2,3,6,11,18,27,38,51,66]

#Annotation, set title, xlabel, ylabel
gp.title("Example 1: plot of x-y data")
gp.xlabel("x,...")
gp.ylabel("y,...")

#Call plot method
gp.plot(x,y)
gp.script("pause 1 'press any key to continue...'")

#...............................................................................
# Example 2: set title, xlabel, ylabel and other otions
#...............................................................................

# reset gnuplot and clear all previous setting
gp.reset()

# Create data, using python list comprehension
import math
sin=math.sin
exp=math.exp
pi=math.pi
x=gp.linspace(0,4.*pi,100)
y=[exp(-xi/6.0)*sin(3.0*xi) for xi in x]


#Annotation, set title, xlabel, ylabel
gp.title("Example 2: plot exp(-x).sin(x)")
gp.xlabel("x,...")
gp.ylabel("y,...")

#set gnuplot options
gp.options="set style data lp"
gp.options="unset key"
#.. multi-line options
##gp.options="""
##set xrange [-10.0:10.0]
##set yrange [-2.0:98.0]
##"""

#Call plot method
gp.plot(x,y,'pt 7')
gp.script("pause 1 'press any key to continue...'")
# close gnuplot
#gp.close()


#...............................................................................
# Example 3: use colors, fonts keys, and range
#...............................................................................

# Create data, using python list comprehension
cos=math.cos
def mydata(n=50):
	"""A helper function to create data for plot demo"""
	dx=2.*pi/(n-1)
	x=[-pi+i*dx for i in range(n)]
	y=[sin(xi) for xi in x]
	z=[cos(xi) for xi in x]
	t=[cos(xi+pi/4) for xi in x]
	w=[cos(xi-pi/4) for xi in x]
	return x, y, z, t, w
#call mydata to create vectors of data
x, y, z, t, w=mydata(40)


#reset gnuplot
gp.reset()
gp.options="""
set style data lp
set xrange [-pi:pi]
set yrange [-1:1]
"""
#Annotation, set title, xlabel, ylabel and using fonts and RGB color format
gp.title('Example 3: multi plot','#009900','Courier,12')
gp.xlabel('x, radian...','#992288','calibri,12')
gp.ylabel('y, [unitless]...','#0000FF','Courier,10')

#Call plot method
gp.plot(
	x,y,'with linesp title "sin(x)" lt 1 pt 1',
	x,z,'title "cos(x)" pt 7',
	x,t,'title "cos(x+pi/4)" pt 11',
	x,w,'title "cos(x-pi/4)" pt 9'
)
gp.script("pause 1 'press any key to continue...'")

#...............................................................................
# Example 4: plot a  matrix versus a vector
#...............................................................................

# Create data from above example
Y=[y,z,t]  #This is a matrix

#reset gnuplot
gp.reset()

gp.options="""
set style data lp
"""
#Annotation, set title, xlabel, ylabel and using fonts and RGB color format
gp.title('Example 4: matrix plot')
gp.ylabel('y, z, t','purple')
gp.xlabel('x, radian','red')

#set keys (legends)
#lspec has to be a tuple
lspec=(
	'title "sin(x)"',
	'title "cos(x)" lt 6 lc rgb "#AA0066"',
	'title "cos(x+pi/4)" lt 2 pt 7'
	)
#Call plot method
gp.plot(x,Y,lspec)
gp.script("pause 1 'press any key to continue...'")

#...............................................................................
# Example 5: plot two series, one series is a single point
#...............................................................................

# Define a helper function to create data
def fun5(x, a=1., b=1., c=1.):
	"""a second order function"""
	return a*x*x+b*x+c

#Series 1
x=gp.linspace(-10,10,25)
a=1.; b=-4.; c=-5.
y=[fun5(xi,a,b,c) for xi in x]

#Series 2: positive root of fun 5
xr=[5.0]
yr=[0.0]

#reset gnuplot
gp.reset()
gp.options="""
set style data lines
set grid
"""
#Annotation, set title, xlabel, ylabel and using fonts and RGB color format
gp.title('Example 5: plot two series, one series has only a single point')
gp.ylabel('x','blue')
gp.xlabel('y','dark-red')

#line specification
lspec=(
'title "ax2+bx+c" lc rgbcolor "blue"',
'title "positive root" with points lc rgbcolor "dark-red" pt 7 ps 1',
)
#Call plot method to draw two series, the second series as red circle
gp.plot(x,y,lspec[0],xr,yr,lspec[1])
gp.script("pause 1 'press any key to continue...'")
#...............................................................................
# Example 6: function plot by fplot
#...............................................................................

# Define a custom function to be plotted
def fun6(x):
	return x*sin(x)


#reset gnuplot
gp.reset()
gp.options="""
set style data lines
"""
#Annotation, set title, xlabel, ylabel and using fonts and RGB color format
gp.title('Example 6: function plot using fplot')
gp.ylabel('x.sin(x)','#990000')
gp.xlabel('x, radian','#000099')

#set keys (legends)
lspec=('title "x.sin(x)" lc rgbcolor "brown"')

#Call fplot method
gp.fplot(fun6,[-10.0,10.0],lspec,N=100)
gp.script("pause 1 'press any key to continue...'")
#...............................................................................
# Example 7: function plot by fplot when function returns more than one value
#...............................................................................

# Define a custom function to be plotted
def fun7(x):
	"""a function returns multi-values"""
	return x*sin(1.2*x), -x*sin(1.2*x), x*cos(1.2*x)


#reset gnuplot
gp.reset()
gp.options="""
set style data lines
"""
#Annotation, set title, xlabel, ylabel and using fonts and RGB color format
gp.title('Example 7: fplot for function with multi-value return')
gp.ylabel('x.sin(x)','#990000')
gp.xlabel('x, radian','#000099')

#set keys (legends)
gp.options="set key top center"
#lspec has to be a tuple
lspec=(
'title "x.sin(x)" lc rgbcolor "brown"',
'title "-x.sin(x)" lc rgbcolor "blue"',
'title "x.cos(x)" lc rgbcolor "dark-green"'
)
#Call fplot method
gp.fplot(fun7,[-10.0,10.0],lspec,N=75)
gp.script("pause 1 'press any key to continue...'")
#...............................................................................
# Example 8: A simple polar plot
#...............................................................................

#1. reset gplot
gp.reset()

#2. set option
gp.options="""
set polar
set trange [-pi/2:pi/2]
set style data lines
"""

#3. helper function to create data
def fun8(t):
	"""helper function to calculate sin(3t) and cos(3t)"""
	return sin(3.0*t)

#4. create data
t=gp.linspace(-pi/2.,pi/2.,75)
r=[fun8(ti) for ti in t]

#Annotation, set title, xlabel, ylabel
gp.title("Example 8: simple polar plot")
gp.xlabel("x,...")
gp.ylabel("y,...")

#Call plot method
gp.plot(t,r,"lc rgb '#8899AA'")
gp.script("pause 1 'press any key to continue...'")
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 3D plotting


#...............................................................................
# Example 11: Simple 3D plot using splot
#...............................................................................

#reset gnuplot
gp.reset()
gp.options="""
set style data lines
unset key
"""


#Create data
X, Y=gp.meshgrid([0,1,3],[0,1,2])
Z=[
	[10,10,10,10],
	[10,5,1,0],
	[10,10,10,10]
]
#Annotation, label colors are also set!
gp.title('Example 11: Simple 3D plot using splot')
gp.xlabel('x,...','#BBAA00')
gp.ylabel('y,...','#00AA00')
gp.zlabel('z,...','#EE0000')

#plot the 3D data
gp.splot(X,Y,Z)
gp.script("pause 1 'press any key to continue...'")


#...............................................................................
# Example 12: A 3D plot using splot
#...............................................................................

#1. reset gnuplot
gp.reset()

#2. set options
gp.options="""
set style data lines
unset key
set xyplane relative 0.1
"""

#3. define a custom function to be plotted
def fun3d12(x,y):
	a=0.5
	b=2.0
	return x**2/a - y**2/b

#4. dreate data
X, Y=gp.meshgrid([-10,2,10],[-10,2,10])
m, n=len(X), len(X[0])
#4.1 calculate Z with nested loops, i-loop acts as outer loop.
#This is equivalent to:
#for i in range(m)
# for j in range(n)
#  ....
Z=[ [fun3d12(X[i][j],Y[i][j]) for j in range(n)] for i in range(m) ]

#5. nnotation
gp.title('Example 12: A 3D plot using splot','orange')
gp.xlabel('x,...')
gp.ylabel('y,...')
gp.zlabel('z,...')

#6. se fsplot to draw the z=f(x,y)
gp.splot(X,Y,Z,'title "x2/a - y2/b" lc rgb "#993300')
gp.script("pause 1 'press any key to continue...'")

#...............................................................................
# Example 14: 3D plot using fsplot
#...............................................................................

#1. reset gnuplot
gp.reset()
gp.options="""
set style data lines
unset key
set xyplane relative 0.1
"""

#2. define a custom function to be plotted
def fun3d14(x,y):
	a=0.5
	b=2.0
	return x*x+y*y #x**2/a - y**2/b
#3. annotation
gp.title('Example 14: 3D function plot using fsplot','brown')
gp.xlabel('x,...')
gp.ylabel('y,...')
gp.zlabel('z,...')

#4. use fsplot to draw the z=f(x,y)
gp.fsplot(fun3d14,[-10,2,10],[-10,2,10],'title "x2/a - y2/b" lc rgb "#00AA00')
gp.script("pause 1 'press any key to continue...'")

#Close gp
gp.close()
 #include "math.h"
// #include <gsl/gsl_fit.h>
// #include <gsl/gsl_statistics.h>
//#include <iomanip>
#include <fstream>
#include <iostream>
#include <ostream>
#include <stdio.h>


using std::cout;
using std::endl;
//using std::setw;

using std::ifstream;
using std::ofstream;

int main()
{
  cout << "Compute Rosenbrock function" << endl;

  ifstream indata; // indata is like cin
  double x; // variable for input value
  double y;
  std::string a;
  indata.open("test.in"); // opens the file
  if(!indata) { // file couldn't be opened
    std::cerr << "Error: file could not be opened" << endl;
  }
  indata >> a >> x;
  indata >> a >> y;
  indata.close();

  double fun = 100.0 * pow( y - pow(x, 2), 2) + pow( 1 - x, 2);
  cout << "fun val = " << fun << endl;

  ofstream myfile;
  myfile.open ("test.out");
  myfile << fun << endl;
  myfile.close();

  cout << "main ended" << endl; 
}

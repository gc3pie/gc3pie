! compute V[i,n] = F(V, i, n) for given values of "i" (state) and "n" (iteration)
!
! this is (ideally) the only place in this program that needs to be changed
!
SUBROUTINE compute(newV, V, i, i0, i1, i2, n, beta)
  ! state to compute; "i" is the index in the "V" array, the
  ! corresponding result will go into newV(i - i0 + 1)
  integer, intent(in) :: i
  ! lowest possible value for "i" in this invocation of the program;
  ! used to shift down indices from "V" to "newV"
  integer, intent(in) :: i0
  ! highest possible value for "i" in this invocation of the program;
  ! used to shift down indices from "V" to "newV"
  integer, intent(in) :: i1
  ! highest possible index in "V", i.e., the size of the "V" array
  integer, intent(in) :: i2
  ! iteration number
  integer, intent(in) :: n
  ! discount factor
  double precision, intent(in) :: beta
  ! array holding the result values; indices in range 1 .. (i1 - i0 + 1)
  double precision, dimension(1:(i0-i1+1)), intent(out) :: newV
  ! array holding the input values; indices in range 1 .. i2
  double precision, dimension(1:i2), intent(in) :: V

  ! dividend
  double precision :: d
  d = i + 1/(n+100)

  if (i .eq. 1) then
     ! bottom edge (hence, i - i0 + 1 == 1)
     newV(i - i0 + 1) = d + beta*(0.5*V(1)+0.5*V(2))
  else if (i .eq. i2) then
     ! top edge (hence, i1 == i2 and i - i0 + 1 == i2 - i0 + 1)
     newV(i - i0 + 1) = d + beta*(0.5*V(i-1) + 0.5*V(i))
  else
     ! now do the general case
     pm1 = 1.0/3.0 - (i+n) / (100*(i1+n))
     p0 = 1.0/3.0
     pp1 = 1.0/3.0 + (i+n) / (100*(i1+n))
     
     ! apply F
     newV(i - i0 + 1) = d + beta*(pm1*V(i-1) + p0*V(i) + pp1*V(i+1))
  end if

END SUBROUTINE compute


PROGRAM vfi

	IMPLICIT NONE 

	INTEGER :: OpenStatus, i, currI, N, IterNum, TotalIters, low, high
	double precision :: beta
	double precision :: d, pm1, p0, pp1
	double precision, dimension(:), allocatable :: V, newV

        ! read command line arguments
        ! (see: http://www2.cs.uh.edu/~johnson2/args.html for an explanation)

        ! a buffer to hold a single command line argument
        ! since all command line arguments are integer numbers,
        ! 200 digits do suffice for all practical purposes
        CHARACTER*200 :: buffer

        ! check that we have enough arguments to parse
        if (IARGC() .ne. 6) then
           print *, "*** Expected 6 arguments, got", IARGC(), "instead ***"
           print *, ""
           print *, "Usage: program BETA ITERATION TOTAL_ITERATIONS N LOW HIGH"
           stop
        end if

        ! 1st argument is the discount factor
        call getarg(1, buffer)
        read (buffer, *) beta

        ! 2nd argument is number of current iteration
        call getarg(2, buffer)
        read (buffer, *) IterNum

        ! 3rd argument is total number of iterations
        call getarg(3, buffer)
        read (buffer, *) TotalIters

        ! 4th argument is number N of items in the ValuesIn.txt file
        call getarg(4, buffer)
        read (buffer, *) N

        ! 5th and 6th arguments are the limits of the range
        ! assigned to this worker
        call getarg(5, buffer)
        read (buffer, *) low
        call getarg(6, buffer)
        read (buffer, *) high
        

	! the worker needs to read the data file. 
	! open the file "ValuesIn.txt"
	open (unit = 30, file = "ValuesIn.txt", status = "old", &
		  action = "read", position = "rewind", iostat = OpenStatus)
	if (OpenStatus > 0) then
		print *, " *** Cannot open file ValuesIn.txt ***"
		stop		
	end if

        ! fill V with values read from the ValuesIn.txt file
	allocate(V(N))
	do i = 1, N
           read (30, *) V(i)
	end do

	! close the ValuesIn.txt file
	close(30)

        ! only allocate enough slots for newV
        allocate(newV(high - low + 1))
        do i = low, high
           newV(i - low + 1) = -777.0
        end do

        ! fill newV(:) by applying function F to V(low:high)
        do currI = low, high
           call compute(newV, V, currI, low, high, N, IterNum, beta)
        end do

	! the worker saves the results
	! open the file "ValuesOut.txt"
	open (unit = 50, file = "ValuesOut.txt", status = "old", &
		action = "write", position = "rewind", iostat = OpenStatus)
	if (OpenStatus > 0) then 
		open (unit = 50, file = "ValuesOut.txt", status = "new", &
			action = "write", position = "rewind", iostat = OpenStatus)
		if (OpenStatus > 0) then 
			print *," *** Cannot open ValuesOut.txt file for writing ***"	
			STOP
		end if
	end if
 
        ! output values
        do i = low, high 
           write (50, *) newV(i - low + 1)
        end do

	! close the file
	close(50)

	! Deallocate spaces 
	deallocate(V)
        deallocate(newV)

!********************
!** End of Program **
!********************
end program vfi

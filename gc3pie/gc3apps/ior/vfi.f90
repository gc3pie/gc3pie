PROGRAM vfi

	IMPLICIT NONE 

	INTEGER :: OpenStatus, i, currI, N, IterNum, TotalIters, low0, high0, low1, high1
	double precision, PARAMETER :: beta = 0.9
	double precision :: d, pm1, p0, pp1
	double precision, dimension(:), allocatable :: V, newV

        ! read command line arguments
        ! (see: http://www2.cs.uh.edu/~johnson2/args.html for an explanation)

        ! a buffer to hold a single command line argument
        ! since all command line arguments are integer numbers,
        ! 200 digits do suffice for all practical purposes
        CHARACTER*200 :: buffer

        ! check that we have enough arguments to parse
        if (IARGC() .ne. 5) then
           print *, "*** Expected 5 arguments, got", IARGC(), "instead ***"
           print *, ""
           print *, "Usage: program N ITERATION TOTAL_ITERATIONS LOW HIGH"
           stop
        end if

        ! 1st argument is number N of items in the Values.txt file
        call getarg(1, buffer)
        read (buffer, *) N

        ! 2nd argument is number of current iteration
        call getarg(2, buffer)
        read (buffer, *) IterNum

        ! 3rd argument is total number of iterations
        call getarg(3, buffer)
        read (buffer, *) TotalIters

        ! 4th and 5th arguments are the limits of the range
        ! assigned to this worker
        call getarg(4, buffer)
        read (buffer, *) low0
        call getarg(5, buffer)
        read (buffer, *) high0
        

	! the worker needs to read the data file. 
	! open the file "Values.txt"
	open (unit = 30, file = "Values.txt", status = "old", &
		  action = "read", position = "rewind", iostat = OpenStatus)
	if (OpenStatus > 0) then
		print *, " *** Cannot open file Values.txt ***"
		stop		
	end if

        ! fill V with values read from the Values.txt file
	allocate(V(N))
	do i = 1, N
		read (30, *) V(i)
	end do

	! close the Values.txt file
	close(30)

        ! only allocate enough slots for newV
        allocate(newV(high0 - low0 + 1))
        do i = 1, high0 - low0 + 1
           newV(i) = -99.0
        end do

        ! fill newV(:) by applying function F to V(low0:high0)

        ! treat edge cases first, so the compiler is free to vectorize the general case
        if (low0 .eq. 1) then
           ! bottom edge
           d = low0 + 1/(IterNum+100)
           newV(1) = d + beta*(0.5*V(1)+0.5*V(2))
           low1 = low0 + 1
        else
           low1 = low0
        end if

        if (high0 .eq. N) then
           ! top edge
           d = N + 1/(IterNum+100)
           newV(high0) = d + beta*(0.5*V(N-1) + 0.5*V(N))
           high1 = high0 - 1
        else
           high1 = high0
        end if

        ! now do the general case
        do currI = low1, high1
           ! dividend
           d = currI + 1/(IterNum+100)
           
           ! iteration
           pm1 = 1.0/3.0 - (currI+IterNum) / (100*(N+IterNum))
           p0 = 1.0/3.0
           pp1 = 1.0/3.0 + (currI+IterNum) / (100*(N+IterNum))

           ! apply F
           newV(currI - low0 + 1) = d + beta*(pm1*V(currI-1) + p0*V(currI) + pp1*V(currI+1))
        end do

	! the worker saves the results
	! open the file "SolVal.txt"
	open (unit = 50, file = "SolVal.txt", status = "old", &
		action = "write", position = "rewind", iostat = OpenStatus)
	if (OpenStatus > 0) then 
		open (unit = 50, file = "SolVal.txt", status = "new", &
			action = "write", position = "rewind", iostat = OpenStatus)
		if (OpenStatus > 0) then 
			print *," *** Cannot open SolVal.txt file for writing ***"	
			STOP
		end if
	end if
 
        ! output values
        do i = 1, high0 - low0 + 1
           write (50, *) newV(i)
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

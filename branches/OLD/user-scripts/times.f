C   Program to calculate the average time per final energy calculation in an optimization
C         by Laura Berstis
C 
C   Compile with: gfortran -o times times.f
C   Run with: times <gamess output file>
C
      PROGRAM TIMEofCYCLE
      IMPLICIT DOUBLE PRECISION (A-H,O-Z)
      CHARACTER*80 useroutput, userinput, aline, bline, contents
      character*80 useroutput2, nodes
      DOUBLE PRECISION currentvalue, nextvalue, total, difference, days
      DOUBLE PRECISION average, hours, totaltime, atoms, nhessians
      DOUBLE PRECISION hoursremaining, daysremaining, energiesleft
      INTEGER p, q, r, s, n, runtype, method, cnsymmetry, iargc
C
C  checking for command line argument input
      CHARACTER *100 BUFFER
!GET THE PARAMETERS FROM THE COMMAND LINE ARGUMENT
      m=iargc()
      IF(m.eq.1) THEN
         CALL GETARG(1,BUFFER)
         userinput=BUFFER
         n=LEN_TRIM(userinput)
      WRITE(6,2222) userinput
 2222 FORMAT(2X, '\n Your file provided is:   ', A80)
         GOTO 102
      ELSE
         GOTO 101
      ENDIF
C      READ(BUFFER,IOSTAT=KODE) D
C         IF(KODE.ne.0) THEN
C            GOTO 101
C         ELSE
C            userinput=D
C            GOTO 102
C         ENDIF
101   WRITE(6,*) '\n Please provide your output file name for which you
     *want to calculate the Final energy cycle time: \n'
      READ(5,*) userinput
      n=LEN_TRIM(userinput)
C
C      WRITE(6,2222) userinput
C 2222 FORMAT(2X, '\n Your output file provided is:   ', A80)

102   OPEN(UNIT=11,FILE=userinput,STATUS='OLD',IOSTAT=KODE)
        IF(KODE.ne.0) THEN
           WRITE(6,*)userinput,' cannot be opened or does not exist'
           GOTO 101
        ENDIF
      IR=11
      useroutput=userinput(1:n)//'.times'
      OPEN(UNIT=9,FILE=useroutput,STATUS='NEW')
      IW=9
      useroutput2=userinput(1:n)//'.temp'
      OPEN(UNIT=13,FILE=useroutput2,STATUS='NEW')
C      OPEN(UNIT=13,STATUS='SCRATCH')

      WRITE(6,*)'\n*************************************************\n
     *  \n     OPTIMIZATION CYCLE TIME CALCULATION PROGRAM         \n'
C
C.........................................................................
C    READ THE TIMES AFTER ONE FINAL ENERGY CYCLE INTO A TEMP FILE
C
      DO p=1,999999999
           READ(IR,'(A80)', IOSTAT=KODE) aline
                IF(KODE.ne.0) THEN
                     WRITE(6,*)'\n   No more data read. Assuming end of
     *file reached.'
                     GOTO 50
                ELSEIF(aline(9:30).eq.'END OF RHF CALCULATION') THEN
                     READ(IR,'(A80)') bline
                     READ(IR,'(A80)') bline
                     WRITE(13,*) bline(24:34)
                ELSEIF(aline(2:22).eq.'TOTAL NUMBER OF ATOMS') THEN
                READ(aline(49:52),'(F4.0)') atoms
                ELSEIF(aline(22:36).eq.'RUNTYP=OPTIMIZE') THEN
                     runtype=1
                ELSEIF(aline(22:35).eq.'RUNTYP=HESSIAN') THEN
                     runtype=2
                ELSEIF(aline(6:19).eq.'METHOD=SEMINUM') THEN
                     method=1
                ELSEIF(aline(2:11).eq.'Initiating') THEN
                     nodes=aline(13:14)
                ELSEIF(aline(20:29).eq.'gracefully') THEN
                     GOTO 50
                ELSEIF(aline(30:47).eq.'NAXIS= 2, ORDER= 2') THEN
                     cnsymmetry=2
                ELSEIF(aline(30:47).eq.'NAXIS= 3, ORDER= 3') THEN
                     cnsymmetry=3
                ELSEIF(aline(30:47).eq.'NAXIS= 4, ORDER= 4') THEN
                     cnsymmetry=4
                ELSEIF(aline(30:47).eq.'NAXIS= 5, ORDER= 5') THEN
                     cnsymmetry=5
                ELSEIF(aline(30:47).eq.'NAXIS= 6, ORDER= 6') THEN
                     cnsymmetry=6
                ELSEIF(aline(1:3).eq.'end') THEN
                GOTO 50
                ENDIF
      ENDDO
C
 50    WRITE(6,*) 'Found all time stamps in file.\n '
      WRITE(13,*)'end of selected time stamps'
C
      CLOSE(UNIT=13, STATUS='KEEP')

C  Counter for the number of lines of time-stamps in the temp file "13"
C...........................................................
C
      s=0
C
      OPEN(UNIT=13,FILE=useroutput2,STATUS='OLD')
      DO r=1,999999999
           READ(13,'(A80)') contents
           IF(contents(2:7).ne.'end of')THEN
                s=s+1
           ELSEIF(contents(2:7).eq.'end of')THEN
C           WRITE(6,*) '\n\n found end of file \n '
           GOTO 70
           ENDIF
      ENDDO
 70   WRITE(6,*)'For reference there should be ',s,' FINAL energies calc
     *ulated in your file.'
      CLOSE(UNIT=13)
C
C....................................................................
C   Start writing final output file
C
      WRITE(IW,4000)
 4000 FORMAT(2X,'\n Welcome to your output file!!!\n ')
      WRITE(IW,4001)
 4001 FORMAT(2X,'\n These are the times required for each successive the
     * FINAL ENERGY cycle calculations. (final average is at the end)')
      WRITE(IW,4006) nodes
 4006 FORMAT('\n This job was run on ',A3,' nodes.')
C
C.........................................................................
C   taking the difference between the lines in the temp file and write to output
C
      OPEN(UNIT=13,FILE=useroutput2,STATUS='OLD')
      currentvalue=0
      total=0
      DO q=1,s
           READ(13,'(F15.1)') nextvalue
           difference=nextvalue-currentvalue
           WRITE(IW,'(F15.1)') difference
           total=total+difference
           currentvalue=nextvalue
      ENDDO
C
C.........................................................................
C  printing the average time per RHF cycle to the output and screen
C
      average=total/s
      hours=average/3600
      WRITE(IW,4002) average, hours
 4002 FORMAT(2X,'The average time per energy calculation time was:',
     *F14.2,' seconds =',F10.2,' hours \n\n')
      WRITE(6,4003) average, hours
 4003 FORMAT(2X,'\n\nThe average time per energy calculation time was:',
     *F14.2,' seconds =',F10.2,' hours. ')
C
C.........................................................................
C      Additional predictions for a seminum hessian calc
C
      IF(runtype.eq.2.and.method.eq.1.) THEN
      nhessians=3*atoms
      totaltime=nhessians*hours
      days=totaltime/24

      IF(cnsymmetry.eq.2) THEN
           nhessians=nhessians/2
           totaltime=totaltime/2
           days=days/2
      ELSEIF(cnsymmetry.eq.3) THEN
           nhessians=nhessians/3
           totaltime=totaltime/3
           days=days/3
      ELSEIF(cnsymmetry.eq.4) THEN
           nhessians=nhessians/4
           totaltime=totaltime/4
           days=days/4
      ELSEIF(cnsymmetry.eq.5) THEN
           nhessians=nhessians/5
           totaltime=totaltime/5
           days=days/5
      ELSEIF(cnsymmetry.eq.6) THEN
           nhessians=nhessians/6
           totaltime=totaltime/6
           days=days/6
      ENDIF

      WRITE(IW,4004) nhessians, atoms, totaltime, days
 4004 FORMAT(2X,'In your seminumeric hessian calculation you will be cal
     *culating\n',F8.0,' final energies for ',F4.0,' atoms,\n  giving
     *a total computation time of approximately',F14.1,' hours \n
     * or',F5.1,' days. \n')
      energiesleft=nhessians-s+1
      hoursremaining=energiesleft*hours
      daysremaining=hoursremaining/24
      WRITE(IW,4005) s, energiesleft, hoursremaining, daysremaining
 4005 FORMAT(2X,'\n  So far there are ',I4,' final energies calculated,
     *leaving\n', F8.0,'  more energies, which will take approximately',
     *F14.1,' more hours or\n',F5.1,' days.\n\n')
      ENDIF
C
C...........................................................................
C
      WRITE(6,5000) useroutput
 5000 FORMAT(2X,'\n\nYour results are now located in your current direct
     *ory in the file \n ', A,' \n\n Have a RADICAL Day!\n\a')
      WRITE(6,*)'\n**************************************************\n'
      STOP
C
C   Closing/deleting the "scratch" file
C
      CLOSE(UNIT=13,STATUS='DELETE')
C
      END PROGRAM TIMEofCYCLE


#!/usr/bin/env perl
#
# Codeml wrapper for the Grid environment
#
# Last update:
# 20-06-2011 AK: added check for command-line arguments
# 12-04-2011 AK: added hostname and cpuinfo to stdout
# $Id: codeml.pl 1225 2011-06-20 15:08:24Z akuzniar $

use strict;
use warnings;

my $bin         = 'codeml';                 # CODEML binary
my $pwd         = $ENV{'PWD'};              # current path
my $RTE         = $ENV{'CODEML_LOCATION'};  # CODEML Runtime Environment (RTE)
my $host        = GetHostname();            # get hostname 
my $cpuinfo     = GetCPUinfo();             # get CPU information
my $binmode     = 0555;                     # file permissions
my $CODEML;
my @temp_files  = qw/rub rst rst1 4fold.nuc lnf 2NG.t 2NG.dS 2NG.dN/;
my $exit_status = 0;

print "----- start of DEBUG info ----\n";
print "host: [$host]\n";
foreach my $key (sort keys %ENV) {
  print "$key=$ENV{$key}\n";
};

print "------\n[$RTE]\n";

print "----- end of DEBUG info ----\n";

# Prepend fullpath to codeml binary and check the run-time environment (RTE) is set
if ($RTE) {
    $CODEML = "$RTE/$bin";
} else {
    $CODEML = "$pwd/$bin";
    die "ERROR: Executable '$CODEML' not found.\n" unless -e $CODEML;
    unless (-x $CODEML) { # set file permissions
        chmod $binmode, $CODEML or die "Error: Failed to set execution permissions on the '$CODEML' file.\n";
    }
};


# Run CODEML sequentially for all control files specified on the
# command line; this could be used, e.g., for testing the null (H0)
# and alternative (H1) hypotheses.
die "Usage: $0 [CONTROL FILE 1]... [CONTROL FILE n]\n" if @ARGV == 0;

foreach my $ctl (@ARGV) {
        print "$0: Running '$CODEML $ctl'...\n";
        print "HOST: $host\n";
        print "CPU: $cpuinfo\n";
        
        # remove temporary files left from previous runs
        unlink @temp_files;

        # try running CODEML
        system($CODEML, $ctl);

        if ($? == -1) {
                # could not execute program: exit now, there is no sense in
                # continuing...  Exit code 127 is what the `bash` shell uses
                # when trying to run a command that cannot be found
                exit 127;
        } elsif ($? != 0) {
                $exit_status++;
                warn "ERROR: Cannot run $CODEML on file '$ctl'.\n";
        }
}


# exitcode tracks number of failed invocations;
# so exit code 0 means "everything OK".
exit $exit_status;


sub GetCPUinfo {
        my $cpuinfo = `grep "model name" /proc/cpuinfo | cut -f 2 -d : | sort -u`;
        chomp $cpuinfo;
        return $cpuinfo;
}

sub GetHostname {
        my $host = `hostname`; # N.B.: $ENV{HOSTNAME} does not work!
        chomp $host;
        return $host;
}

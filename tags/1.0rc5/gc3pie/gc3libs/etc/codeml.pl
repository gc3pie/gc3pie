#!/usr/bin/perl
#
# CODEML wrapper for the Grid environment
#

use strict;
use warnings;

my $bin = 'codeml';
my $pwd = $ENV{PWD};                    # current path
my $RTE = $ENV{'CODEML_LOCATION'};      # CODEML Runtime Environment (RTE)
my $binmode = 0755;                     # file permissions
my $CODEML;
my @temp_files  = qw/rub rst rst1 4fold.nuc lnf 2NG.t 2NG.dS 2NG.dN/;

# Check if the CODEML RTE is set on the site otherwise use the (copied) binary in the current path
if ($RTE) {
    $CODEML = $bin;
}
else {
    $CODEML = "$pwd/$bin";
    die "ERROR: Executable '$CODEML' not found.\n" unless -e $CODEML;
    unless (-x $CODEML) { # Make the binary executable if needed
        chmod $binmode, $CODEML or die "Error: Failed to set execution permissions on the '$CODEML' file.\n";
    }
};

# Run CODEML sequentially for all control files specified on the
# command line; this could be used, e.g., for testing the null (H0)
# and alternative (H1) hypotheses.
my $failed = 0;
foreach my $ctl (@ARGV) {
    print "codeml.pl: Running '$CODEML $ctl' ...\n";
    # remove temporary files left from previous runs
    unlink @temp_files;
    # try running CODEML
    system($CODEML, $ctl);
    if ($? == -1) {
        # could not execute program: exit now, there is no sense in
        # continuing...  Exit code 127 is what the `bash` shell uses
        # when trying to run a command that cannot be found
        exit 127;
    }
    elsif ($? != 0) {
        $failed++;
        warn "ERROR: Cannot run $CODEML on file '$ctl'.\n";
    };
};

# exitcode tracks number of failed invocations;
# so exit code 0 means "everything OK".
exit $failed;

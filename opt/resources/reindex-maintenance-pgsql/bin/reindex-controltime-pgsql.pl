#!/usr/bin/perl
#reindex-controltime-pgsql
#
#Description: Rotina de controle de manutenção de REINDEX para o PostgreSQL
#
#Author:
#        Gabriel Prestes (helkmut@gmail.com)
#
#08-09-2013 : Created

use strict;
use Getopt::Long;
use POSIX;
use File::Basename;
use warnings;

#--------------------------------------------------
# Setting environment
#--------------------------------------------------
$ENV{"USER"}="root";
$ENV{"HOME"}="/root";

#--------------------------------------------------
# Global variables
#--------------------------------------------------
our $name = basename($0);
our $version = "0.1";
our $opt_path = "/opt/resources/reindex-maintenance-pgsql";
our $log_date = `/bin/date -I`;
chomp($log_date);
our $temp_log = "$opt_path/log/reindex-controltime-pgsql-$log_date.log";
our $opt_props = "$opt_path/lib/maindb.props";
our ($opt_help, $opt_verbose, $opt_version);

#--------------------------------------------------
# Program variables
#--------------------------------------------------

#--------------------------------------------------
# Prop variables
#--------------------------------------------------
our $opt_pguser;
our $opt_pgport;
our $opt_pgdb;
our $opt_fim;

#--------------------------------------------------------------------------------------

sub main {

        # --- Get Options --- #
        getoption();

        # --- Init function vars ---#
        my $counter = 0;
        my $flagcontrol=0;
        my $rows=0;
        my $sqlquery;
        my $cmd;
        my @prop_split=();
        my @props_array=();
        my @command=();
        my @sqlcmd = ();

        # --- Verbose ---#
        logger("|PROGRAM OUT: Init agent|");

        # --- Get props program --- #
        open (PROPS, "$opt_props") or error();
        @props_array = <PROPS>;
        close(PROPS);

        foreach(@props_array){

                chomp($_);
                @prop_split = split(/=/,$_);
                if($counter == 0){$opt_pguser = $prop_split[1];}
                if($counter == 1){$opt_pgport = $prop_split[1];}
                if($counter == 2){$opt_pgdb = $prop_split[1];}
                if($counter == 4){$opt_fim = $prop_split[1];}
                $counter++;

        }

        $counter=0;

        # --- Check and write pid --- #
        if(check_pid() == 0){

                logger("|PROGRAM OUT: Another job in execution($opt_path/var/reindex-controltime-pgsql.pid)|");
                exit(1);

        } else {

                write_pid();

        }

        # --- Rotate logs more than 15 days --- #
        logger("|PROGRAM OUT: LOGs - Search for more than 15 days old|");
        $cmd=`\$\(which find\) $opt_path/log/* -name "*" -mtime +15 -exec \$\(which rm\) -rf {} \\; > /dev/null 2>&1`;


        # --- Get relations to reindex --- #
        logger("|PROGRAM OUT: Obtendo relacao de reindex em execucao|");
        $sqlquery = "SELECT pg_stat_activity.procpid as processo FROM pg_stat_activity WHERE pg_stat_activity.current_query LIKE 'CREATE INDEX CONCURRENTLY idx_temp_maintenance%';";

        @sqlcmd = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -q -c \"$sqlquery\"`;
        $flagcontrol+=$?;

        if($flagcontrol>0){

                logger("|PROGRAM OUT: Nao pode obter lista de indices : falha de comunicacao com a instancia|");
                exit_program();

        }

        # --- Mount tables in array --- #
        foreach(@sqlcmd){

                chomp($_);
                if($_ =~ m/^ (.+)$/g){

                        push(@command,$1);

                }

                if($_ =~ m/^\((.+) row.+$/){

                        $rows=$1;

                }

        }

        my @sqlcmdaux = ();
        trim($rows);

        if(@command){

                foreach(@command){

                        chomp($_);

                        if($_ !~ m/^proces.+$/){

                        	trim($_);
                                logger("|PROGRAM OUT: O processo$_ de recriacao do indice sera cancelado|");
				@sqlcmd = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -q -c \"SELECT pg_cancel_backend($_)\"`;

                        }


                }


        }

        logger("|PROGRAM OUT: $rows reindex cancelado(s)|");

        if($rows==0){

                logger("|PROGRAM OUT: OK - Rotina de controle finalizada sem cancelamento de backends!|");
                exit_program();

        }

        # --- Threshoulds --- #
        logger("|PROGRAM OUT: OK - Rotina de controle finalizada com sucesso!|");
        exit_program();

}

#--------------------------------------------------------------------------------------

sub getoption {
     Getopt::Long::Configure('bundling');
     GetOptions(
            'V|version'                 => \$opt_version,
            'h|help'                    => \$opt_help,
            'v|verbose=i'               => \$opt_verbose,
        );

     if($opt_help){

             printHelp();
             exit_program();

     }

     if($opt_version){

             print "$name - '$version'\n";
             exit_program();

     }

}

#--------------------------------------------------------------------------------------

sub logger {

        return(0) if (not defined $opt_verbose);

        my $msg = shift (@_);
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
        $wday++;
        $yday++;
        $mon++;
        $year+=1900;
        $isdst++;

        if ($opt_verbose == 0){

                         print "$msg\n";

        } else {

           open(LOG, ">>$temp_log") or error();
           printf LOG ("%02i/%02i/%i - %02i:%02i:%02i => %s\n",$mday,$mon,$year,$hour,$min,$sec,$msg);
           close(LOG);

        }

}

#--------------------------------------------------------------------------------------

sub printHelp {

                my $help = <<'HELP';


                Thanks for use Reindex Controltime PGSQL.

                API required:

                                strict;
                                Getopt::Long;
                                POSIX;
                                File::Basename;
                                warnings;

                Agent binary           : bin/reindex-controltime-pgsql.pl &
                Configuration Agent in : lib/maindb.props
                Support                : helkmut@gmail.com

		"Cordeiro de Deus, retirai os pecados do mundo, tende piedade de nós!"




HELP

                system("clear");
                print $help;

}

#--------------------------------------------------------------------------------------

sub error {

        print "|ERROR - Unexpected return - contact support|\n";
        exit_program();

}

#--------------------------------------------------------------------------------------

sub trim($) {

        my $string = shift;

        $string =~ s/^\s+//;
        $string =~ s/\s+$//;

        return $string;

}

#--------------------------------------------------------------------------------------

sub write_pid {

        my $cmd;

        $cmd=`\$\(which touch\) $opt_path/var/reindex-controltime-pgsql.pid`;

        return 1;

}

#--------------------------------------------------------------------------------------

sub check_pid {

        if(-e "$opt_path/var/reindex-controltime-pgsql.pid"){

                return 0;

        } else {

                return 1;

        }

}

#--------------------------------------------------------------------------------------

sub exit_program {

        my $cmd;

        $cmd=`\$\(which rm\) -rf $opt_path/var/reindex-controltime-pgsql.pid`;

        exit;

}

#--------------------------------------------------------------------------------------

&main


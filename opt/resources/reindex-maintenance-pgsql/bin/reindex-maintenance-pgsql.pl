#!/usr/bin/perl
#reindex-maintenance-pgsql
#
#Description: Rotina de manutenção de REINDEX para o PostgreSQL
#
#Author:
#        Gabriel Prestes (helkmut@gmail.com)
#
#Version control:
#07-31-2013 : Created - (version: 1.0)
#08-07-2013 : Modified(LM² consult) - (version: 1.2)
#09-02-2013 : Modified(Bug fix drop index) - (version: 1.2)
#09-03-2013 : Modified(Schema bug fix and database arg) - (version: 1.3)
#09-05-2013 : Modified(Set public schema) - temporary fix
#09-12-2013 : Add verbose
#10-02-2013 : Fix dbview index - (version: 1.4)
#11-13-2013 : Fix constraint get - (version: 1.5)
#
#Wish features:
#
# 1 - Adicionar flag para schemas ou ajustar rotina para atender corretamente os schemas;(adicionado na versão 1.4)
# 2 - Realizar rotina em multiplas databases.(adicionado na versão 1.3)

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
our $version = "1.5";
our $opt_path = "/opt/resources/reindex-maintenance-pgsql";
our $log_date = `/bin/date -I`;
chomp($log_date);
our $temp_log = "$opt_path/log/reindex-maintenance-pgsql-$log_date.log";
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
        my @tables=();
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
                if($counter == 2){if(!$opt_pgdb){$opt_pgdb = $prop_split[1];}}
                if($counter == 3){$opt_fim = $prop_split[1];}
                $counter++;

        }

        $counter=0;

        # --- Check and write pid --- #
        if(check_pid() == 0){

                logger("|PROGRAM OUT: Another job in execution($opt_path/var/reindex-maintenance-pgsql.pid)|");
                exit(1);

        } else {

                write_pid();

        }

        # --- Rotate logs more than 15 days --- #
        logger("|PROGRAM OUT: LOGs - Search for more than 15 days old|");
        $cmd=`\$\(which find\) $opt_path/log/* -name "*" -mtime +15 -exec \$\(which rm\) -rf {} \\; > /dev/null 2>&1`;

        # --- Drop idx_temp_maintenance if exists --- #
        my @sqlcmdaux = ();
        my $sqlindexpartnew = "idx_temp_maintenance";
        logger("|PROGRAM OUT: Drop $sqlindexpartnew if exists|");
        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT 'DROP INDEX IF EXISTS '||nspname||'.'||i.relname||';' FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::"char" AND i.relkind = 'i'::"char" AND indisprimary IS FALSE AND i.relname='$sqlindexpartnew';\" | /usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb`;
        $flagcontrol += $?;
        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT 'DROP INDEX IF EXISTS '||nspname||'.'||i.relname||';' FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" AND indisprimary IS FALSE AND i.relname='$sqlindexpartnew';\" | /usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb");

        # --- Get relations to reindex --- #
        logger("|PROGRAM OUT: Obtendo relacao de tabelas mais utilizadas com indices inchados|");

        my $sqlindexquery;
        my $sqlin;
        my $sqlindexpart1;
        my $sqlindexpart2;
        my $sqlindexpart3;
        my $time=`/bin/date +%H`;
        my $objidx;
        my $indexcount=0;
        my $total=0;
        chomp($time);

        # --- Build REINDEX command --- #
        logger("|PROGRAM OUT: Montando vetor de tabelas...|");
        logger("|PROGRAM OUT: Obtendo relacao de indices...|");

        $sqlquery="SELECT REPLACE(Pg_get_indexdef(i.oid), 'INDEX ', 'INDEX CONCURRENTLY ') || ';' FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r' AND i.relkind = 'i' AND x.indisprimary IS FALSE AND x.indisvalid IS TRUE AND c.relname IN(SELECT DISTINCT ON (tablename) tablename FROM bloat_objects WHERE (tablename IN ( SELECT relname FROM pg_stat_user_tables WHERE (n_tup_upd+n_tup_del) > 0 ORDER BY (n_tup_upd+n_tup_del) DESC LIMIT 30 )) AND perc_ibloat > 50::numeric ORDER BY tablename, perc_ibloat DESC) AND NOT EXISTS (SELECT 1 FROM pg_constraint z WHERE z.conname = i.relname AND z.conrelid = c.oid);";

        @sqlcmd = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"$sqlquery\"`;
        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"$sqlquery\"");

        if($#sqlcmd == 0){

                logger("|PROGRAM OUT: OK - Nenhuma tabela para reindexar rotina de reindexacao realizada com sucesso!|");
                exit_program();

        }

        logger("|PROGRAM OUT: Retorno de DDL criacao dos indices:|");
        $total = $#sqlcmd+1;

        foreach(@sqlcmd){

                chomp($_);
                logger("$_");

        }

        # --- Run reindex --- #
        foreach(@sqlcmd){

                $counter++;
                $sqlindexquery = "";
                $sqlindexquery = $_;

                if($sqlindexquery =~ m/^(CREATE.+CONCURRENTLY) (.+) (ON.+)$/){

                        $sqlindexpart1=$1;
                        $sqlindexpart2=$2;
                        $sqlindexpart3=$3;

                        logger("|PROGRAM OUT: Recriando indice: $sqlindexpart2 (index $counter of $total)|");

                        # --- Time check --- #
                        $time=`/bin/date +%H`;
                        chomp($time);
                        if($time>=$opt_fim){

                                logger("|PROGRAM OUT: Recriacao do indice abortado em funcao do horario: $time|");
                                next;

                        } else {

                                logger("|PROGRAM OUT: Horario verificado nao excede gatilho: $time|");
                                $indexcount++;

                        }

                        logger("|PROGRAM OUT: $sqlindexpart1 $sqlindexpartnew $sqlindexpart3|");
                        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -q -c \"$sqlindexpart1 $sqlindexpartnew $sqlindexpart3\"`;
                        $flagcontrol += $?;
                        if($flagcontrol>0){logger("|PROGRAM OUT: Falha na criacao do indice temporario $sqlindexpartnew|"); next;}
                        $objidx = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT nspname FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::"char" AND i.relkind = 'i'::"char" AND indisprimary IS FALSE AND i.relname='$sqlindexpart2';\"`;
                        $flagcontrol += $?;
                        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT nspname FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" AND indisprimary IS FALSE AND i.relname='$sqlindexpart2';\"");
                        chomp($objidx);
                        logger("|PROGRAM OUT: AJUST INDEX $objidx.$sqlindexpart2|");
                        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -q -c \"BEGIN ; SET SCHEMA \'$objidx\'; DROP INDEX $sqlindexpart2 ; ALTER INDEX $sqlindexpartnew RENAME TO $sqlindexpart2 ; COMMIT ;\"`;
                        $flagcontrol += $?;
                        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -q -c \"BEGIN ; SET SCHEMA \'$objidx\'; DROP INDEX $sqlindexpart2 ; ALTER INDEX $sqlindexpartnew RENAME TO $sqlindexpart2 ; COMMIT ;\"");

                }

        }

        if($indexcount==0){

                logger("|PROGRAM OUT: ALERTA - Rotina abortada em funcao do horario: $time|");
                exit_program();

        }

        # --- ANALYZE tables --- #
        foreach(@tables){

                trim($_);

                if($_ !~ "tablename"){

                        chomp($_);
                        logger("|PROGRAM OUT: Atualizando as estatisticas da tabela '$_'|");
                        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT 'ANALYZE '||schemaname||'.'||relname||';' FROM pg_stat_user_tables WHERE relname LIKE '$_';\" | /usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb`;
                        $flagcontrol += $?;
                        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT 'ANALYZE '||schemaname||'.'||relname||';' FROM pg_stat_user_tables WHERE relname LIKE '$_';\" | /usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb");

                }

        }

        # --- Temp index check after routine --- #
        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT nspname||'.'||i.relname FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::"char" AND i.relkind = 'i'::"char" AND indisprimary IS FALSE AND i.relname='$sqlindexpartnew';\"`;
        logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT nspname||'.'||i.relname FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" AND indisprimary IS FALSE AND i.relname='$sqlindexpartnew';\"");

        if(!@sqlcmdaux){

                logger("|PROGRAM OUT: $sqlindexpartnew not exist|");

        } else {

                foreach(@sqlcmdaux){

                        chomp($_);
                        logger("|PROGRAM OUT: Index temp: $_|");
                        @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT 'DROP INDEX IF EXISTS '||nspname||'.'||i.relname||';' FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace WHERE  c.relkind = 'r'::"char" AND i.relkind = 'i'::"char" AND indisprimary IS FALSE AND i.relname='$sqlindexpartnew';\" | /usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb`;
        $flagcontrol += $?;

                }

        }

        # --- Reset stats --- #
        my $datetmp=`/bin/date +%w`;
        chomp($datetmp);
        if($datetmp==0){

                logger("|PROGRAM OUT: Resetando estatisticas|");
                @sqlcmdaux = `/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT pg_stat_reset();\"`;
                logger("/usr/bin/psql -U $opt_pguser -p $opt_pgport $opt_pgdb -Atq -c \"SELECT pg_stat_reset();\"");

        }

        # --- Threshoulds --- #
        if($flagcontrol>0){

                logger("|PROGRAM OUT: ERRO - Rotina de reindexacao finalizou com erros($flagcontrol) verifique o mais rapido possivel|");

        } else {

                logger("|PROGRAM OUT: OK - Rotina de reindexacao realizada com sucesso!|");

        }

        exit_program();

}

#--------------------------------------------------------------------------------------

sub getoption {
     Getopt::Long::Configure('bundling');
     GetOptions(
            'D|database=s'             => \$opt_pgdb,
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


                Thanks for use Reindex Maintenance PGSQL.

                API required:

                                strict;
                                Getopt::Long;
                                POSIX;
                                File::Basename;
                                warnings;

                Agent binary           : bin/reindex-maintenance-pgsql.pl &
                Configuration Agent in : lib/maindb.props
                Support                : helkmut@gmail.com

                Salmo 91:

                "Direi do Senhor: Não temerás os terrores da noite, nem a seta que voe de dia, nem peste que anda na escuridão, nem mortandade que assole ao meio-dia."





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

        $cmd=`\$\(which touch\) $opt_path/var/reindex-maintenance-pgsql.pid`;

        return 1;

}

#--------------------------------------------------------------------------------------

sub check_pid {

        if(-e "$opt_path/var/reindex-maintenance-pgsql.pid"){

                return 0;

        } else {

                return 1;

        }

}

#--------------------------------------------------------------------------------------

sub exit_program {

        my $cmd;

        $cmd=`\$\(which rm\) -rf $opt_path/var/reindex-maintenance-pgsql.pid`;

        exit;

}

#--------------------------------------------------------------------------------------

&main

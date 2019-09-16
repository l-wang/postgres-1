#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Config;
use File::Path qw(rmtree);
if ($Config{osname} eq 'MSWin32')
{

	# some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
else
{
	plan tests => 6;
}

my $node = get_new_node('master');
$node->init(allows_streaming => 1);
$node->start;

my ($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that txid_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
my $tx = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
BEGIN;
CREATE TABLE mine(x integer);
SELECT txid_current();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
my $xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'in progress', 'own xid is in-progress');

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT txid_current()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$tx->kill_kill;

# Ensure that tablespace removal doesn't cause error while recovering
# the preceding create database with that tablespace.

my $node_master = get_new_node('master2');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create tablespace
my $tspDir_master = TestLib::tempdir;
my $realTSDir_master = TestLib::perl2host($tspDir_master);
$node_master->safe_psql('postgres', "CREATE TABLESPACE ts1 LOCATION '$realTSDir_master'");

my $tspDir_standby = TestLib::tempdir;
my $realTSDir_standby = TestLib::perl2host($tspDir_standby);

# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name,
					 tablespace_mappings =>
					   "$realTSDir_master=$realTSDir_standby");
my $node_standby = get_new_node('standby2');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure connection is made
$node_master->poll_query_until(
	'postgres', 'SELECT count(*) = 1 FROM pg_stat_replication');

# Make sure to perform restartpoint after tablespace creation
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->safe_psql('postgres', 'CHECKPOINT');

# Do immediate shutdown just after a sequence of CREAT DATABASE / DROP
# DATABASE / DROP TABLESPACE. This leaves a CREATE DATBASE WAL record
# that is to be applied to already-removed tablespace.
$node_master->safe_psql('postgres',
						q[CREATE DATABASE db1 WITH TABLESPACE ts1;
						  DROP DATABASE db1;
						  DROP TABLESPACE ts1;]);
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->stop('immediate');

# Should restart ignoring directory creation error.
is($node_standby->start(fail_ok => 1), 1);

# Ensure that a missing tablespace directory immediately causes panic if standby
# has already reached recovery consistency.

$node_master = get_new_node('master4');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create tablespace
$tspDir_master = TestLib::tempdir;
$realTSDir_master = TestLib::perl2host($tspDir_master);
$node_master->safe_psql('postgres', "CREATE TABLESPACE ts1 LOCATION '$realTSDir_master'");

$tspDir_standby = TestLib::tempdir;
$realTSDir_standby = TestLib::perl2host($tspDir_standby);

# Take backup
$backup_name = 'my_backup';
$node_master->backup($backup_name,
					 tablespace_mappings =>
					   "$realTSDir_master=$realTSDir_standby");
$node_standby = get_new_node('standby4');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure standby reached consistency and starts accepting connections
$node_standby->poll_query_until('postgres', 'SELECT 1', '1');

# Pause replication
$node_standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');

# Remove standby tablespace directory so it will be missing when replay resumes
rmtree($realTSDir_standby);

# Create a database in the tablespace and a table in default tablespace
$node_master->safe_psql('postgres',
						q[CREATE TABLE should_not_replay_insertion(a int);
						  CREATE DATABASE db1 WITH TABLESPACE ts1;
						  INSERT INTO should_not_replay_insertion VALUES (1);]);

# Resume replication on standby and make sure it's caught up
$node_standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));

# Standby should panic and should not silently skip replaying the wal
is($node_standby->safe_psql('postgres', 'SELECT count(*) = 0 FROM should_not_replay_insertion'),
	't', 'insertion should not be replayed');

# Ensure that tablespace removal doesn't cause error while recovering the
# preceding alter database set tablespace.

$node_master = get_new_node('master3');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Create tablespace
$tspDir_master = TestLib::tempdir;
$realTSDir_master = TestLib::perl2host($tspDir_master);
mkdir "$realTSDir_master/1";
mkdir "$realTSDir_master/2";
$node_master->safe_psql('postgres', "CREATE TABLESPACE ts1 LOCATION '$realTSDir_master/1'");
$node_master->safe_psql('postgres', "CREATE TABLESPACE ts2 LOCATION '$realTSDir_master/2'");

$tspDir_standby = TestLib::tempdir;
$realTSDir_standby = TestLib::perl2host($tspDir_standby);

# Take backup
$backup_name = 'my_backup';
$node_master->backup($backup_name,
					 tablespace_mappings =>
					   "$realTSDir_master/1=$realTSDir_standby/1,$realTSDir_master/2=$realTSDir_standby/2");
$node_standby = get_new_node('standby3');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming => 1);
$node_standby->start;

# Make sure connection is made
$node_master->poll_query_until(
	'postgres', 'SELECT count(*) = 1 FROM pg_stat_replication');

$node_master->safe_psql('postgres', "CREATE DATABASE db1 TABLESPACE ts1");

# Make sure to perform restartpoint after tablespace creation
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->safe_psql('postgres', 'CHECKPOINT');

# Do immediate shutdown ...
$node_master->safe_psql('postgres',
						q[ALTER DATABASE db1 SET TABLESPACE ts2;
						  DROP TABLESPACE ts1;]);
$node_master->wait_for_catchup($node_standby, 'replay',
							   $node_master->lsn('replay'));
$node_standby->stop('immediate');

# Should restart ignoring directory creation error.
is($node_standby->start(fail_ok => 1), 1);

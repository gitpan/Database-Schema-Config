package Database::Schema::Config;

use 5.008007;
use strict;
use warnings;
use Class::ParmList qw(simple_parms parse_parms);
use Time::Timestamp;

our $VERSION = '.01';
use constant TABLE => 'config';

=head1 NAME

Database::Schema::Config - Perl extension for storing generic config strings with revision control in a table

=head1 SYNOPSIS

This is an interface module to our database. All SQL queries should be done at this level and only leave the actual config parsing to the upper level modules.

*Note: All references to timestamp or date/time are usually stored as Time::Timestamp objects, see Time::Timestamp for output options.

=head1 DESCRIPTION

An API for storing and manipulating configuration files RCS-style using a database backend. This allows the author to utilize any Config module they wish (config::General, Config::Simple, etc...).

=head1 SQL Table [mysql]

  -- 
  -- Table structure for table `config`
  -- 

  CREATE TABLE `config` (
    `rev` int(11) NOT NULL auto_increment,
    `xlock` tinyint(4) NOT NULL default '0',
    `dt` int(11) NOT NULL default '0',
    `user` varchar(32) NOT NULL default '',
    `config` text NOT NULL,
    `log` text,
    PRIMARY KEY  (`rev`)
  ) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=2 ;

=head1 OBJECT METHODS

=head2 new()

Constructor

  my $cfg = Database::Schema::Config->new(
  	-dbh => $myDBI_handler,
  	-str => $configString,
  	-user => $user,
  	-table => 'myConfigTable',
  );

=cut

sub new {
	my ($class,%parms) = @_;
	die('No DBH Defined!') if(!$parms{-dbh});
	$parms{-table} = TABLE() if(!$parms{-table});
	my $self = {};
	bless($self,$class);
	$self->init(%parms);
	return $self;
}

# INIT

sub init {
	my ($self,%parms) = @_;
	$self->table(	$parms{-table});
	$self->dbh(	$parms{-dbh});
	$self->user(	$parms{-user});
	$self->rev(	$parms{-rev});
	$self->string(	$parms{-string});
}

# METHODS

=head2 listConfigs()

Fetch a listing of all of the stored configs. The listing will contain the rev, timestamp, lock status, and user. If you want the  log and config, use getConfig().

Returns

 (undef,HASHREF)       	on success containing keys: "rev", "timestamp",
                	"lock", "user". Each of those point to ARRAYREFs.
 ('db failure',undef)   something failed with the DB

So the revision of the first config in the list (which should be the oldest) is $hr->{'rev'}->[0]

=cut

sub listConfigs {
    	my $self = shift;
    	my $sql  = 'SELECT rev, dt AS timestamp, xlock, user FROM config ORDER BY rev ASC';
    	my $rv   = $self->dbh->selectall_arrayref($sql);

	return ("db failure ".$self->dbh->errstr(),undef) unless(ref($rv eq 'ARRAY') || ($#{$rv} > -1));

    	my $hv   = { 'rev' => [], 'timestamp' => [], 'lock' => [], 'user' => [] };
    	foreach my $row (@$rv) {
	    	push @{$hv->{'rev'}},       $row->[0];
	    	push @{$hv->{'timestamp'}}, Time::Timestamp->new(ts => $row->[1]);
	    	push @{$hv->{'lock'}},      $row->[2];
	   	push @{$hv->{'user'}},      $row->[3];
    	}
    	return (undef,$hv);
}

=head2 isConfigLocked()

Check to see if the config is currently locked. If it is, return information about the lock.

  $cfg->isConfigLocked();

Returns

  (undef,0)		not locked
  (undef,HASHREF)	locked. see keys for details.
  ('db failure',undef)	something failed with the DB

=cut

sub isConfigLocked {
    	my $self = shift;

   	my $sql = 'SELECT rev, user FROM config WHERE xlock = 1';
    	my $rv  = $self->dbh->selectall_arrayref($sql);

	return 'db failure: '.$self->dbh->errstr() unless(ref($rv) eq 'ARRAY');

        $self->errstr('multiple locks on config detected.') unless($#{$rv} < 2);
    	return (undef,0) if ($#{$rv} == -1);  # no locks

    	return (undef,{
		'rev'  => $rv->[0]->[0],
             	'user' => $rv->[0]->[1],
	});
}

=head2 lockConfig()

Lock the configuration so other people know we are editting it. A note
will be appended to the "log" for the configuration.  The latest
configuration will be "locked" unless "rev" is specified.

 $cfg->lockConfig(-rev => $rev, -user => $username);

=over 4

=item +rev

The revision to lock. Required. Pass in the revision of the currently running config.

=item user

An identifier denoting who is locking the config. Required

=back

Returns

 (undef,1)             		on success
 ('lock failed',0)        	someone has it locked already. check the log by fetching
                      		the config. See C<NetPass::DB::getConfig>
 ('invalid parameters',undef) 	the routine was called improperly
 ('db failure',undef)         	something failed with the DB

=cut

sub lockConfig {
    	my $self = shift;

    	my $parms = parse_parms({
		-parms => \@_,
		-required => [ qw(-rev -user) ],
		-defaults => {
			   -rev    => $self->rev(),
			   -user   => $self->user(),
		}
	});

    	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) if (!defined($parms));

	my ($r, $u) = $parms->get('-rev', '-user');

	return ('invalid parameters (rev)',undef) unless($r >= 0);
	return ('invalid parameters (user)',undef) unless($u ne '');

	my $isLocked = $self->isConfigLocked();
	return ('lock failed: already Locked rev='.$isLocked->{rev}.' user='.$isLocked->{user},undef) unless(ref($isLocked) ne 'HASH');

   	my $sql = 'UPDATE config SET xlock = 1, user = '.$self->dbh->quote($u).' WHERE rev = '.$self->dbh->quote($r);
    	my $rv  = $self->dbh->do($sql);

	return ('db failure: '.$self->dbh->errstr(),undef) unless(defined($rv));

    	$self->appendLogToConfig(
		-rev => $r,
		-user => $u,
		-log => ['config locked']
	);
    	return (undef,1);
}

=head2 unlockConfig()

Unlock the configuration. Both parameters are required.

 $cfg->unlockConfig(-rev => $rev, -user => $username);

Returns

 (undef,1)             		on success
 ('invalid parameters',undef) 	the routine was called improperly
 ('config not locked',0)	the config is not locked
 ('db failure',undef) 		something failed with the DB

=cut

sub unlockConfig {
    	my $self = shift;

    	my $parms = parse_parms({
		-parms => \@_,
		-required => [ qw(-rev -user) ],
		-defaults => {
			-user   => $self->user(),
			-rev	=> $self->rev(),
		}
	});

	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) unless(defined($parms));

	my ($r, $u) = $parms->get('-rev', '-user');
	$r = 0 if(!defined($r));

	return ('invalid parameters (rev)',undef) unless($r >= 0);
	return ('invalid parameters (rev)',undef) unless(defined($u) && $u ne '');
	return ('Config is not locked',0) unless($self->isConfigLocked() eq 'HASH');

	my $rv = $self->appendLogToConfig(
		-rev => $r,
		-user => $u,
		-log => ['config unlocked'],
	);
    	return ($rv,undef) unless($rv);

    	my $sql = 'UPDATE config SET xlock = 0 WHERE rev = '.$self->dbh->quote($r).' AND user = '.$self->dbh->quote($u);
    	$rv = $self->dbh->do($sql);
    	return ('db failure: '.$self->dbh->errstr(),undef) unless($rv);
    	return (undef,1);
}

=head2 appendLogToConfig()

  $cfg->appendLogToConfig(-rev => rev, -user => username, -log => []);

Add a log entry to the given config revision.

Returns

 (undef,1)             		on success
 (undef,0)			Revision doesn't exist
 ('invalid parameters',undef)	the routine was called improperly
 ('db failure',undef)		something failed with the DB

=cut

sub appendLogToConfig {
	my $self = shift;

    	my $parms = parse_parms({
		-parms => \@_,
		-required => [ qw(-rev -user -log) ],
		-defaults => {
			-rev    => $self->rev(),
			-user   => $self->user(),
			-log    => []
		}
	});

	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) unless(defined($parms));

	my ($r, $u, $l) = $parms->get('-rev', '-user', '-log');

	return ('invalid parameters (rev)',undef) unless($r >= 0);
	return ('invalid parameters (user)',undef) unless(defined($u) && $u ne '');
	return ('log empty',0) unless((ref($l) eq 'ARRAY') && ($#{$l} >= 0)); #empty?

	my $sql = 'SELECT `log` FROM `config` WHERE `rev` = '.$self->dbh->quote($r);
	my $rv = $self->dbh->selectall_arrayref($sql);

	return ('db failure: '.$self->dbh->errstr(),undef) unless(ref($rv) eq 'ARRAY');

	if ($#{$rv} == -1) {
	    	# the revision didnt exist. we dont throw an error tho.
	    	return ('revision doesnt exist',0);
    	}

	$rv->[0]->[0] ||= '';

	my $l2  = join('', scalar(localtime())." $u\n", @$l, "\n", $rv->[0]->[0]);

	$sql = 'UPDATE config SET log = ? WHERE rev = ?';
	my $sth = $self->dbh->prepare($sql);
	$rv = $sth->execute($l2,$r);

	return ('db failure: '.$self->dbh->errstr(),undef) if(!$rv);
	return (undef,1);
}

=head2 getConfig()

Fetch the specified configuration from the database. If "rev" is not
give, fetch the highest (latest) config from the database. If "lock"
is "1", place an advisory lock on the configuration so that other people
can't edit it without a warning.

  $cfg->getConfig(-rev => integer, -user => $username, -lock => [0|1]);


=over 4

=item +rev

An optional integer identifying which configuration to retrieve from the database. Default is to fetch the latest.

=item user

This parameter is required of lock is "1".

=item lock

 0 = get the config, I don't plan on editting it. (DEFAULT)
 1 = get the config, I plan on editting it, so warning anyone else
     who tries to edit the config.

=back

Returns

 (undef,HASHREF)       	containing keys:
                  	{
				'config'    => ARRAYREF,
                    		'log'       => ARRAYREF,
                    		'timestamp' => integer,
                    		'rev'       => integer,
                    	'	user'      => scalar string
                  	}
 ('lock failed',undef)  you said lock=1 but someone else already has a config locked for editting
 ('db failure',undef)   something failed with the DB

=cut

sub getConfig {
    	my $self = shift;

    	my $parms = parse_parms({
		-parms => \@_,
		-required => [ qw() ],
		-defaults => {
			-rev    => $self->rev(),
			-lock   => 0,
			-user   => $self->user(),
		}
	});

	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) unless(defined($parms));

    	my ($r, $l, $u) = $parms->get('-rev', '-lock', '-user');

    	$r ||= 0;

    	return ('invalid parameters (rev)',undef) unless($r >= 0);
    	return ('invalid parameters (lock)',undef) unless($l == 0 || $l == 1);
    	return ('invalid parameters (user)',undef) if(($l == 1) && ($u eq ''));

	my $rv;
    	if ($l) {
	    	$rv = $self->lockConfig(-rev => $r, -user => $u);
	    	return $rv unless($rv);
    	}

    	my $sql = 'SELECT config, log, dt AS timestamp, rev, user FROM config ';
    	$sql .= ' WHERE rev = '.$self->dbh->quote($r) if $r;
    	$sql .= ' WHERE rev = (select MAX(rev) FROM config)' if ($r == 0);

    	$rv = $self->dbh->selectall_arrayref($sql);

    	return ('db failure '.$self->dbh->errstr(),undef) unless(ref($rv) eq 'ARRAY');
	return ('db empty',undef) if($#{$rv} < 1);
    	return (undef,{
		'config'    	=> [ split("\n", $rv->[0]->[0]) ],
	      	'log'       	=> [ split("\n", $rv->[0]->[1]) ],
	       	'timestamp' 	=> Time::Timestamp->new(ts => $rv->[0]->[2]),
	       	'rev'       	=> $rv->[0]->[3],
	       	'user'      	=> $rv->[0]->[4],
	});
}

=head2 putConfig()

Insert a new configuration file into the database ("config" table). It's up to the calling application to "notice" the config rev was updated.

 $cfg->putConfig(
 	-config => ARRAYREF,
 	-user => "username",
 	-log => ARRAYREF,
	-autounlock => 0, # default is to unlock the config if isConfigLocked() == true
 );

=over 4

=item config

This is an array reference that contains the new configuration file (string).

=item user

A username or identifier of the person who is importing the new configuration.

=item log

An optional array reference containing some text describing what changes have been made.

Returns

 (undef,1)			on success.
 ('db failure',undef)		something failed with the DB
 ('invalid parameters',undef)  	the routine was called improperly.

=back

=cut

sub putConfig {
	my $self = shift;

	my $parms = parse_parms({
		-parms => \@_,
		-required => [qw(-config -user)],
		-legal => [qw(-config -user -lockOverride -autounlock)],
		-defaults => {
			-config    => [],
			-user      => $self->user(),
			-log       => [],
			-lockOverride => 0,
			-autounlock => 1,
		}
	});

	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) if (!defined($parms));

	my ($c,$u,$l,$lo,$au) = $parms->get('-config','-user','-log','-lockOverride','-autounlock');

	return ('invalid parameters (config empty)',undef) unless(ref($c) eq 'ARRAY' && $#{$c} >= 0);
	return ('invalid parameters (user empty)',undef) unless($u ne '');

	my $hr = $self->isConfigLocked();
	return ('no lock on previous config',undef) if(ref($hr) ne 'HASH' && $lo != 1);
 	return ('Someone else has already locked this config: user='.$hr->{user}.' rev='.$hr->{rev},undef) if($self->user() ne $hr->{user});

	my $ts = Time::Timestamp->new(ts => time());
	my $sql = 'INSERT INTO `'.$self->table().'` (dt,user,config) VALUES (?,?,?)';
	my $sth = $self->dbh->prepare($sql);
	my $rv = $sth->execute($ts->epoch(),$u,@$c);

	return ('db failure: '.$self->dbh->errstr(),undef) unless($rv);

	$sql = 'SELECT rev FROM config WHERE user = ? AND dt = ?';
	$sth = $self->dbh->prepare($sql);
	$rv = $sth->execute($u,$ts->epoch());

	return ('db failure: '.$self->dbh->errstr(),undef) unless($rv);

	my @row = $sth->fetchrow_array();

	# append initial message
	my ($err,$rv2) = $self->appendLogToConfig(
		-rev 	=> $row[0],
		-user	=> $u,
		-log 	=> ['created'],
	);
	return ($err,$rv2) if(!$rv2);

	$self->unlockConfig() unless(!$au || !$self->isConfigLocked());

	# append the users log message
	($err,$rv) = $self->appendLogToConfig(
		-rev	=> $row[0],
		-user	=> $u,
		-log	=> $l,
	);
	return ('db failure: '.$self->dbh->errstr(),undef) if(!defined($rv));
	return (undef,1);
}

=head2 resetLocks()

This function resets all xLocks in the event that something screws up.

 $cfg->resetLocks(
 	-rev => $rev, # defaults to $cfg->rev()
 );

=cut

sub resetLocks {
	my $self = shift;

	my $parms = parse_parms({
		-parms => \@_,
		-legal => [qw(-rev)],
		-defaults => {
			-rev => $self->rev(),
		}
	});

	return ("invalid parameters\n".Carp::longmess (Class::ParmList->error()),undef) if (!defined($parms));
	my ($rev) = $parms->get('-rev');

	my $sql = 'UPDATE `'.$self->table().' SET xlock = 0';
	$sql .= ' WHERE rev = '.$self->dbh->quote($rev) if($rev);

	my $rv = $self->dbh->do($sql);
	return ('database failed: '.$self->dbh->errstr(),undef) unless($rv);
	return (undef,1);
}

# ACCESSORS / MODIFIERS

=head2 dbh()

Sets and returns the Database handle

=cut

sub dbh {
	my ($self,$v) = @_;
	$self->{_dbh} = $v if(defined($v));
	return $self->{_dbh};
}

=head2 table()

Sets and returns the base config table

=cut

sub table {
	my ($self,$v) = @_;
	$self->{_table} = $v if(defined($v));
	return $self->{_table};
}

=head2 string()

Sets and returns the config string

=cut

sub string {
	my ($self,$v) = @_;
	$self->{_string} = $v if(defined($v));
	return $self->{_string};
}

=head2 user()

Sets and returns the user

=cut

sub user {
	my ($self,$v) = @_;
	$self->{_user} = $v if(defined($v));
	return $self->{_user};
}

=head2 rev()

Sets and returns the rev

=cut

sub rev {
	my ($self,$v) = @_;
	$self->{_rev} = $v if(defined($v));
	return $self->{_rev};
}

1;
__END__

=head1 SEE ALSO

Time::Timestamp

sourceforge://netpass

=head1 AUTHOR'S

Original Author - Jeff Murphy - E<lt>jcmurphy@buffalo.eduE<gt>

Stolen By - Wes Young - E<lt>saxguard9-cpan@yahoo.comE<gt>

=head1 LICENSE

   (c) 2006 University at Buffalo.
   Available under the "Artistic License"
   http://www.gnu.org/licenses/license-list.html#ArtisticLicense

=cut

# Jeff, you're still a wanker..... ;-)

#!/usr/bin/perl

use strict;
use warnings;
use feature qw|say state|;
use utf8;
use FindBin;
use lib "$FindBin::Bin/../modules";

#package Class;
#use Alpha;

#package Child;
#use Alpha;
#use parent -norequire, 'Class';

package main;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use Getopt::Std;
use Storable;
use File::Path qw/remove_tree/;
use List::Util qw/any all/;
use IO::Handle;

use constant LANG => {
	العربية => 'AR',
	中文 => 'ZH',
	Eng => 'EN',
	English => 'EN',
	Français => 'FR',
	Русский => 'RU',
	Español => 'ES',
	Other => 'DE'
};

use constant LANG2 => {
	A => 'AR',
	C => 'ZH',
	E => 'EN',
	F => 'FR',
	R => 'RU',
	S => 'ES',
	O => 'DE'
};

RUN: {
	MAIN(options());
}

sub options {
	my @opts = (
		['h' => 'help'],
		['s:' => 'sql'],
		['S:', => 'script'],
		['d:' => 'save dir'],
		['3:' => 's3 db'],
		['t' => 'sort by bib# asc'],
		['T' => 'sort by bib# desc'],
		['r' => 'redownload technical reissues'],
		['f' => 'force replace s3 file (even if already exists)'],
		['l:' => 'use list of bibs']
	);
	getopts (join('',map {$_->[0]} @opts), \my %opts);
	if (! %opts || $opts{h}) {
		say join ' - ', @$_ for @opts;
		exit; 
	}
	$opts{$_} || die "required opt $_ missing\n" for qw|d 3|;
	-e $opts{$_} || die qq|"$opts{$_}" is an invalid path\n| for qw|3|;
	return \%opts;
}

sub MAIN {
	my $opts = shift;
	
	use Get::ODS;
	use Get::Hzn;
	my $ods = Get::ODS->new;
	my $hzn = Get::Hzn->new;
	my ($dir,$c) = ($opts->{d},0);
	$dir //= '.';
	state $range_control = 0;
	state $s3;
	
	use DBI;
	my $dbh = DBI->connect('dbi:SQLite:dbname='.$opts->{3},'','');
	
	my ($sql,$opt);
	if ($opts->{s}) {
		$sql = 'sql';
		$opt = 's';
	} elsif ($opts->{S}) {
		$sql = 'script';
		$opt = 'S';
	} elsif (my $list = $opts->{l}) {
		open my $in,'<',$list;
		my @ids;
		while (<$in>) {
			chomp;
			my @row = split "\t";
			push @ids, $row[0];
		}
		$sql = 'sql';
		$opt = 's';
		my $ids = join ',', @ids;
		$opts->{s} = "select bib# from bib_control where bib# in ($ids)";
	} else {
		say 'indexing s3 data...';
		my $s3;
		{
			my $sth = $dbh->prepare('select bib,lang from docs');
			$sth->execute;
			while (my $row = $sth->fetch) {
				push @{$s3->{$row->[0]}}, $row->[1];
			}
		}
		
		say 'looking for new Hzn files...';
		
		my %ids;
		my $get = Get::Hzn->new (
			sql => 'select bib#,text from bib where tag = "856" and bib# in (select bib# from bib where tag = "191")',
			encoding => 'utf8'
		);
		open my $list,'>','missing_'.time.'.txt';
		$list->autoflush(1);
		my @results = $get->execute (
			callback => sub {
				my $row = shift;
				my $id = $row->[0];
				my $text = $row->[1] or return;
				return unless $text =~ /ods\.un\.org/;
				my $lang = LANG->{$get->get_sub($text,'3')} || '?';
				return if any {$lang eq $_} @{$s3->{$id}};
				say {$list} join "\t", $id, $lang, ':', @{$s3->{$id}};
				$ids{$row->[0]} //= 1;
			}
		);
		my $ids = join ',', sort {$a <=> $b} keys %ids;
		$sql = 'sql';
		$opt = 's';
		$opts->{s} = "select bib# from bib_control where bib# in ($ids)";
	}
	
	my @ids = Get::Hzn->new($sql => $opts->{$opt})->execute;
	@ids = sort {$a->[0] <=> $b->[0]} @ids if $opts->{t};
	@ids = sort {$b->[0] <=> $a->[0]} @ids if $opts->{T};
	while (@ids) {
		my $chunk = join ',', map {$_->[0]} splice @ids, 0, 1000;
		Get::Hzn::Dump::Bib->new->iterate (
			criteria => qq|select bib# from bib where tag = "856" and bib# in ($chunk)|,
			encoding => 'utf8',
			callback => sub {
				my $record = shift;
				my $range = range($record->id,1000);
				my @syms = $record->get_values('191','a');
				return if all {$_ eq '***'} @syms;
				for my $_856 ($record->get_fields('856')) {
					if ($range_control ne $range) {
						say "***\nrange was: $range_control; is: $range. getting new s3 data chunk...";
						$s3 = s3_data($range);
						$range_control = $range;
					}
					next unless index($_856->get_sub('u'),'http://daccess-ods.un.org') > -1;
					my $lang = LANG->{$_856->get_sub('3')};
					warn "$syms[0] language not detected\n" and next if ! $lang;
					my $_596 = $record->get_values('596','a');
					if ($s3->{$record->id}->{$lang}) {
						if ($opts->{r} && $_596 && $_596 =~ /reissued for technical reasons/i) {
							print "*redownloading technical reissue... ";
						} elsif ($opts->{f}) {
							print "force re-download... ";
						} else {
							say $record->id." $syms[0] $lang already in s3";
							next;
						}	
					}
					$c++;
					mkdir $opts->{d} if ! -e $opts->{d};
					my $save = save_path($opts->{d},$record->id,\@syms,$lang);
					DOWNLOAD: {
						my $result = $ods->download($syms[0],$lang,$save);
						if (! $result && $syms[1]) {
							print "\ttrying second symbol... ";
							$result = $ods->download($syms[1],$lang,$save);
						}
						if (! $result && $_856->get_sub('u') =~ /Lang=([ACEFRSO])/) {
							print "\ttrying alt lang code... ";
							my $lang2 = $1 // '';
							$result = $ods->download($syms[0],$lang2,$save) if $lang2 ne $lang;
						}
						my $bib = $record->id;
						my $key = save_path('Drop/docs_new',$record->id,\@syms,$lang);
						if ($result) {
							print "\t";
							my $ret = system qq|aws s3 mv "$save" "s3://undhl-dgacm/$key"|;
							if ($ret == 0) {
								my $check = $dbh->selectrow_arrayref(qq|select key from docs where bib = $bib and lang = "$lang"|);
								my $sql;
								if ($check->[0]) {
									$sql = qq|update docs set key = "$key" where bib = $bib and lang = "$lang"|;
								} else {
									$sql = qq|insert into docs values($bib,"$lang","$key")|;
								}
								$dbh->do($sql) or die "db error: $@";
								$dbh->do(qq|delete from error where bib = $bib and lang = "$lang"|);
								say "data recorded #";
							} else {
								die "s3 error $?";
							}
						} else {
							say "error recorded #";
							my $check = $dbh->selectrow_arrayref(qq|select key from error where bib = $bib and lang = "$lang"|);
							unless ($check->[0]) {
								$dbh->do(qq|insert into error values($bib,"$lang","$key")|) or die "db error: $@";
							}
						}
					}
					print "\n";
				}
				return;
			}
		);
		#remove_tree($opts->{d});
	}
}

sub save_path {
	my ($dir,$bib,$syms,$lang) = @_;
	my $range = range($bib,1000);
	my $rdir = join '/', $dir,$range;
	mkdir $rdir if ! -e $rdir;
	my $sdir = join '/', $rdir,$bib;
	mkdir $sdir if ! -e $sdir;
	return join '/', $sdir,encode_fn($syms,$lang);
}

sub range {
	my ($bib,$inc) = @_;
	return "1-${inc}" if $bib < $inc;
	my $lo = int ($bib / $inc); # - ($bib % $inc));
	$lo *= $inc;
	#my $hi = $lo + $inc -1; :\
	my $hi = $lo + $inc;
	my $range = join '-', $lo,$hi;
	return $range;
}

sub encode_fn {
	my ($syms,$lang) = @_;
	$lang ||= '';
	my @subbed;
	for (@$syms) {
		my $sub = $_;
		$sub =~ tr/\/\*:/_!#/; 
		push @subbed, $sub;
	}
	return join(';',sort @subbed)."-$lang.pdf";
}

sub s3_data {
	my $range = shift;
	my $return;
	my $cmd = qq|aws s3 ls s3://undhl-dgacm/Drop/docs_new/$range/ --recursive|;
	open my $h,'-|',$cmd;
	while (<$h>) {
		chomp;
		my $path = substr $_,31;
		my $bib = (split /\//, $path)[3];
		my $lang = substr $path,-6,2;
		$return->{$bib}->{$lang} = $path;
	}
	close $h; #close handle to get exit code
	die "s3 read error $?" unless any {$? == $_} 0, 256;
	
	return $return;
}

END {}

__DATA__
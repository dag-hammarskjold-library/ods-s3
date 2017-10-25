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
use List::Util qw/any all/;

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
		['r' => 'redownload technical reissues']
	);
	getopts (join('',map {$_->[0]} @opts), \my %opts);
	if (! %opts || $opts{h}) {
		say join ' - ', @$_ for @opts;
		exit; 
	}
	$opts{$_} || die "required opt $_ missing\n" for qw|d 3|;
	-e $opts{$_} || die qq|"$opts{$_}" is an invalid path\n| for qw|d 3|;
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
							print "*redownloading technical reissue...";
						} else {
							say $record->id." $syms[0] $lang already in s3";
							next;
						}	
					}
					$c++;
					my $save = save_path($opts->{d},$record->id,\@syms,$lang);
					DOWNLOAD: {
						my $result = $ods->download($syms[0],$lang,$save);
						if ($result) {
							print "\t";
							my $key = save_path('Drop/docs_new',$record->id,\@syms,$lang);
							my $ret = system qq|aws s3 mv "$save" "s3://undhl-dgacm/$key"|;
							if ($ret == 0) {
								my $bib = $record->id;
								my $check = $dbh->selectrow_arrayref(qq|select key from keys where bib = $bib and lang = "$lang"|);
								my $sql;
								if ($check->[0]) {
									$sql = qq|update keys set key = "$key" where bib = $bib and lang = "$lang"|;
								} else {
									$sql = qq|insert into keys values($bib,"$lang","$key")|;
								}
								$dbh->do($sql) or die "db error";
								say "data recorded #";
							}
						}
					}
					print "\n";
				}
				return;
			}
		);
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
		$sub =~ tr/\/\*/_!/; 
		push @subbed, $sub;
	}
	return join(';',sort @subbed)."-$lang.pdf";
}

sub s3_data {
	my $range = shift;
	my $return;
	my $cmd = qq|aws s3 ls s3://undhl-dgacm/Drop/docs_new/$range/ --recursive|;
	open my $h,'-|',$cmd;
	#die "s3 read error $?" unless any {$? == $_} 0, 256;
	while (<$h>) {
		chomp;
		my $path = substr $_,31;
		my $bib = (split /\//, $path)[3];
		my $lang = substr $path,-6,2;
		$return->{$bib}->{$lang} = $path;
	}
	return $return;
}

END {}

__DATA__
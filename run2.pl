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
use File::Spec::Functions;
use List::Util qw/first any all/;
use DBI;
use Get::ODS;
use Get::Hzn;

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
	MAIN2(options());
}

sub options {
	my @opts = (
		['h' => 'help'],
		#['s:' => 'sql'],
		#['S:', => 'script'],
		#['d:' => 'save dir'],
		['3:' => 's3 db'],
		#['t' => 'sort by bib# asc'],
		#['T' => 'sort by bib# desc'],
		#['r' => 'redownload technical reissues'],
		#['f' => 'force replace s3 file (even if already exists)'],
		#['l:' => 'use list of bibs'],
		['b:' => 's3 bucket'],
		['d:' => 'directory in bucket'],
		['i:' => 'input file'],
		['b:' => 'bib# to start at (default 1)']
	);
	getopts (join('',map {$_->[0]} @opts), \my %opts);
	if (! %opts || $opts{h}) {
		say join ' - ', @$_ for @opts;
		exit; 
	}
	$opts{$_} || die "required opt $_ missing\n" for qw||;
	-e $opts{$_} || die qq|"$opts{$_}" is an invalid path\n| for qw||;
	return \%opts;
}

sub MAIN2 {
	my $opts = shift;
	
	mkdir 'temp' if ! -d 'temp';
	chdir 'temp';
	
	my $ods = Get::ODS->new;
	my $db = S3::DB->new(bucket => $bucket);
	my $s3 = S3::CLI->new(bucket => $bucket;
	
	my $sql = 'select bib#,tag,"ai",indicators,text,"xr","idk" from bib_with_longtext where tag in ("191","856")';
	$sql .= ' and bib# >= '.$opts->{b} if $opts->{b};
	my $hzn = Get::Hzn->new(sql => $sql);
	$hzn->encoding('utf8');
	
	$hzn->execute (
		iterate => 1,
		callback => \&download
	);
}

sub download {
	my $r = shift;
	my %syms;
	$syms{$_} = 1 for map {s/^\[.*?\]//r} grep {$_ ne '***'} $r->get_values('191','a','z');
	my @syms = keys %syms;
	return unless @syms;
	for my $f (grep {$_->get_sub('u') =~ /(daccess-ods.un.org|documents-dds)/} $r->fields('856')) {
		my $lang = LANG->{ $f->get_sub('3') };
		my $save = save_path('.',$r->id,\@syms,$lang);
		my $lang2 = LANG2->{$1} if $f->get_sub('u') =~ /Lang=([ACEFRSO])/;
		my $job;
		if ($f->get_sub('u') =~ /&DS=([^&]+)/) {
			$job = $ods->download($1,$lang2,$save);
		} else {
			$job = first {$ods->download($_,$lang2,$save)} @syms;
		}
		if ($job) {
			my $key = save_path($opts->{d},$record->id,\@syms,$lang);
			my ($ret,$i,$try) = (0,0,3);
			while ($ret == 0 && $i < $try) {
				$ret = $s3->put($save,$key);
				$i++;
			}
			if ($ret) {
				$db->success();
			} else {
				say 's3 xfer error';
			}
		} else {
			$db->error();
		}		
	}
}

#sub download {
#	my ($bib,$syms,$lang) = @_;
#	
#	my $result = $ods->download($syms[0],$lang,$save);
#	my $key = save_path('Drop/docs_new',$record->id,\@syms,$lang);
#	if ($result) {
#		print "\t";
#		my $ret = system qq|aws s3 mv "$save" "s3://undhl-dgacm/$key"|;
#		die "s3 error $?" if $ret != 0;
#		my $check = $dbh->selectrow_arrayref(qq|select key from docs where bib = $bib and lang = "$lang"|);
#		my $sql;
#		if ($check->[0]) {
#			$sql = qq|update docs set key = "$key" where bib = $bib and lang = "$lang"|;
#		} else {
#			$sql = qq|insert into docs values($bib,"$lang","$key")|;
#		}
#		$dbh->do($sql) or die "db error: $@";
#		$dbh->do(qq|delete from error where bib = $bib and lang = "$lang"|);
#		say "data recorded #";
#	} else {
#		say "error recorded #";
#		my $check = $dbh->selectrow_arrayref(qq|select key from error where bib = $bib and lang = "$lang"|);
#		unless ($check->[0]) {
#			$dbh->do(qq|insert into error values($bib,"$lang","$key")|) or die "db error: $@";
#		}
#	}
#	
#	print "\n";
#}

sub record_success {
	
}

sub record_error {

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
	return join '-', 1,$inc if $bib < $inc;
	my $lo = int ($bib / $inc) * $inc + 1; 
	my $hi = $lo + $inc - 1;
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
#!/usr/bin/perl

use strict;
use warnings;

my $project_home = './';
my $data_dir = "~/dataset";
my $data_zip = "$data_dir/data.zip";

if (!(-e $data_dir and -d $data_dir)) {
    mkdir $data_dir;
}

system "wget -O $data_zip http://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz";
system "tar -xvzf $data_zip -C $data_dir";
system "rm $data_zip";




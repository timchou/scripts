#!/usr/local/roadm/bin/perl
 
use strict;
use warnings;
use DBI;
use DBD::mysql;

use Time::HiRes;

my $host='10.15.1.22';
my $db='mydb';
my $user='dongkai';
my $pass='hello123';

#print "Starting main program\n";
my @childs;

for ( my $count = 1; $count <= 100; $count++) {
        my $pid = fork();
        if ($pid) {
        # parent
        #print "pid is $pid, parent $$\n";
        push(@childs, $pid);
        } elsif ($pid == 0) {
                # child
		if($count <= 80){
                        sub1($count);
                }else{
                        sub2($count);
                }
                exit 0;
        } else {
                die "couldnt fork: $!\n";
        }
 
 
 
}

foreach (@childs) {
	my $tmp = waitpid($_, 0);
#        print "done with pid $tmp\n";
 
}

#print "End of main program\n";

sub sub1 {
	my $dsn = "dbi:mysql:$db:10.15.1.22:3306";
	my $connect = DBI->connect($dsn, $user, $pass);
	
	my $start = Time::HiRes::gettimeofday();	
	for(my $i=0;$i <= 100000;$i++){
		my $query = "SELECT * FROM mytbl where uid=".int(rand(2000000));
		my $query_handle = $connect->prepare($query);

 		$query_handle->execute();
		$query_handle->fetch();
	}
	my $end = Time::HiRes::gettimeofday();

        return $end - $start;
}

sub sub2 {
	my $dsn = "dbi:mysql:$db:10.15.1.22:3306";
	my $connect = DBI->connect($dsn, $user, $pass);
	
	my $start = Time::HiRes::gettimeofday();	
	for(my $i=0;$i <= 999999999;$i++){
		my $query = "UPDATE mytbl set name='sql_updated' where uid=".int(rand(2000000));
		my $query_handle = $connect->prepare($query);

 		$query_handle->execute();
	}
	my $end = Time::HiRes::gettimeofday();

        return $end - $start;
}

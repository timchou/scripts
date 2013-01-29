#!/usr/local/roadm/bin/perl
 
use strict;
use warnings;
use Net::HandlerSocket;
use Time::HiRes;

 
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
	my $args = { host => '10.15.1.22', port => 9998 };
	my $hs = new Net::HandlerSocket($args);
	my $err = $hs->open_index(3, 'mydb', 'mytbl', 'PRIMARY','uid,name,a');
	
	my $start = Time::HiRes::gettimeofday();	
	for(my $i=0;$i <= 999999999;$i++){
		my $res = $hs->execute_single(3, '=', [ int(rand(2000000)) ], 1, 0);
		#print @$res;
	}
	my $end = Time::HiRes::gettimeofday();

        return $end - $start;
}


sub sub2 {
	my $args = { host => '10.15.1.22', port => 9999 };
	my $hs = new Net::HandlerSocket($args);
	my $err = $hs->open_index(4, 'mydb', 'mytbl', 'PRIMARY','name,b');
	
	my $start = Time::HiRes::gettimeofday();	
	for(my $i=0;$i <= 999999999;$i++){
		my $res = $hs->execute_single(4, '=', [ int(rand(2000000)) ], 1, 0,'U',['hs_updated',99]);
	}
	my $end = Time::HiRes::gettimeofday();

        return $end - $start;
}

use strict;
use warnings;

my @files = `find . | grep \.css`;

my $file;
foreach $file ( @files ) {
  chop( $file );
  print STDERR "Currently fixing: " . $file . "\n";

  open(IN, $file);
  open(OUT, ">z");

  my $line;
  while( defined( $line = <IN> ) ) {
    if( $line =~ /^a:visited \{/ ) {
      $line = <IN>;
      print OUT "    color: rgb(183, 97, 78);\n";
    }
    else {
      print OUT $line;
    }
  }

  close(OUT);
  close(IN);

  `mv z $file`;
}

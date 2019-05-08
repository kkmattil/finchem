#!/bin/bash


ds_path=("/opt/chipster/tools_local/BIOVIA/DiscoveryStudio2019")

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$ds_path/lib
export PERL5LIB=$ds_path/lib/5.26.1:$ds_path/lib/site_perl:$ds_path/lib/site_perl/5.26.1:$ds_path/lib

input=$1
output=$2

if ![ -e $input ] ; then
  echo "Error: Input file $input was not fond" 
  echo "Error"
  exit 1
fi
perl ds_prepare_ligands.pl $input $output

if [ -e $output ] ; then
  ls -l $output
  echo $output 
fi


#!/usr/bin/perl -w
# Describes how to launch a protocol.
use strict;
use MdmDiscoveryScript;
use ProtocolDiscoveryScript;

#my $samplesFolder = '/home/kkmattil/Documents/DDCB/finchem/';
my $inputFile = $ARGV[0];
my $outputFile = $ARGV[1];
#my $inputFile = $samplesFolder . $infileName;

# Protocol name and parameters
my $protocolName = 'Prepare Ligands';
my %parameterHash = ( "Input Ligands" =>  $inputFile,
                      "Generate Tautomers Enumerate What" => "CanonicalTautomer",
                     );

my $server = "https://dstudio19.csc.fi:9943";

print "Connecting to server...\n";
my $session = Protocol::Session::Create($server);
#my $session = Protocol::Document::DefaultSession();
if ($session)
{
    print "Connected to $session->{Server} as user $session->{User}.\n";
}
else
{
    die "No Pipeline Pilot server found. Please connect to a valid server and try again.\n";
}

# Print default running folder, which can be changed
print "Running in $session->{RunFolder}...\n";

print "Open protocol document for $protocolName\n";
my $document = Protocol::Document::Create( $protocolName, $session );
  
my $guid = $document->Guid();
print "Protocol guid: $guid\n";

my $version = $document->Version();
print "Protocol version: $version\n";
## Setup the protocol parameters

for my $key (keys %parameterHash) {
     if ( $document->ItemExists($key) ) {
         $document->ReplaceItem( $key, $parameterHash{$key} );
         print "Set parameter '$key' : $parameterHash{$key}\n"; 
     }
     else{
         print "\n<warning> not a valid parameter '$key'\n\n"; 
     }
}

## Launch Pipeline Pilot protocol

my $saveIntermediateFiles = True;
my $maxFilesizeToDownload = 256;
my $showJobCompleteDialog = False;
my $task                  = $session->Launch(
    $document,              $maxFilesizeToDownload,
    $saveIntermediateFiles, $showJobCompleteDialog
);

print "\nLaunch the protocol: $protocolName \n";
print "Task info\n";
print "Name:    $task->{ProtocolName}\n";
print "Host:    $task->{Host}\n";
print "RunPath: $task->{RunPath}\n";
print "TaskId:  $task->{TaskId}\n";

## Track progress of task
print " $protocolName Task is running...\n";
my $times = 0;
while ( $task->IsRunnable() )
{
    print "Status: $task->{State}\n";
    sleep(5);
    ++$times;
    if ( $times > 500 )
    {
        $task->KillTask();
    }
}


$task->WaitForCompletion();


# get the log file
my $messages = $task->LogMessages;
print "$protocolName Log messages:\n";
map { print "$_\n"; } @$messages; 

my $finalStatus = $task->State;
my $finalMolecularData;
if ( $finalStatus eq Protocol::taskComplete )
{
    print "$protocolName completed\n";
    my $path = $task->RunPath;
    my $outDir = "$path/Output";
    print "$outDir\n";
    system("cp \"$outDir/$inputFile\" ./$outputFile");
    #die "Output file was not found please check report!" if ( not -e $outFile );
    # load the results        
    #    DiscoveryScript::Open( { Path => $outFile, LoadAllObjects => True } ); 
}
else
{
    print " $protocolName Task failed with status $finalStatus\n";
}



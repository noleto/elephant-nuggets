#!/bin/bash
# Elephant-Nuggets is a fun tool for packaging files stored in S3 in homogeneous Gzipped parts. We call it "a nugget".
# Elephant-Nuggets relies on Amazon Elastic MapReduce to create a more uniformed files in relation to its size.
#
#Requirements:
# * s3cmd (http://s3tools.org/s3cmd)
# * python
# * elastic-mapreduce tools (http://aws.amazon.com/developertools/2264)
#
#@version 1.0, 2014-08-04
#@author Leonardo Noleto (noleto.leonardo at gmail dot com)
# --------------------------------------------------------------------

EXIT_SUCCESS=0
EXIT_FAILURE=1


#global variables
VERBOSE=0
JOB_FLOW_IDENTIFIER=
CREATE_CLUSTER=0
NB_EC2_INSTANCES=4
EC2_INSTANCE_TYPE=m1.medium
SAFETY_MODE_ENABLED=1
DELETE_RAW_FILES=1

# The ideal size of a nugget
BLOCK_SIZE_BYTES=134217728

RETVAL=0
S3_PATH=
NUGGETED_BASENAME_FILES_LIST=
NUGGETS_TIMESTAMP=
# --------------------------------------------------------------------

help()
{
    echo "Usage: Elephant-Nuggets [options] s3-path-to-package"
    echo
    echo "Elephant-Nuggets is a fun tool for packaging files stored in S3 in homogeneous Gzipped parts. We call it \"a nugget\"".
    echo "It uses Amazon Elastic MapReduce to create a more uniformed files (in relation to its size)."
    echo "Options:"
    echo -e "\t-h\t\t\t show this help message and exit"
    echo -e "\t-j  JOBFLOW_ID:\t\t launch the MapReduce Job in an existing cluster with the given EMR jobflow identifier"
    echo -e "\t-c\t\t\t create a new cluster on EMR and launch the MapReduce. The cluster will automatically be terminated at the end of job"
    echo -e "\t-n NUMBER_INSTANCES\t specify the number of EC2 instances in the cluster (nodes + master). Default is 4 (3 nodes + 1 master)"
    echo -e "\t-t INSTANCE_TYPE\t the type of EC2 instances to launch as the nodes in the cluster. Default is m1.medium"
    echo -e '\t-b NUM_BYTES_BLOCK\t the "ideal" size of a nugget file. As the files to package have unevenly size in the most real use cases, nuggeted files will have final size around the NUM_BYTES_BLOCK bytes'
    echo -e "\t-S \t\t\t disable safety mode. Be careful, when safety mode is disabled directories are allowed inside the S3 path (this is not the normal case)"
    echo -e "\t-d \t\t\t do not delete raw files."
}

#options parsing
OPTIND=1         # Reset in case getopts has been used previously in the shell.
while getopts "j:cn:t:b:Sdvh" opt; do
    case "$opt" in
    h)
        help
        exit $EXIT_SUCCESS
        ;;
    j)  JOB_FLOW_IDENTIFIER=$OPTARG
        ;;
    v)  VERBOSE=1
		echo "Verbose mode is activated"
        ;;
    c) CREATE_CLUSTER=1
        ;;
    n)  
        if [[ "$OPTARG" -eq "$OPTARG" ]] 2>/dev/null; then
            NB_EC2_INSTANCES=$(($OPTARG))
        else
            echo "Please, provide a number of EC2 instances"
            exit $EXIT_FAILURE
        fi
        if [[ $NB_EC2_INSTANCES -lt 2 ]]; then
        	echo "A cluster must have 2 instances at least (1 master + 1 node)"
            exit $EXIT_FAILURE
        fi
        ;;
    t) EC2_INSTANCE_TYPE=$OPTARG
        ;;
    b) if [[ "$OPTARG" -eq "$OPTARG" ]] 2>/dev/null; then
            BLOCK_SIZE_BYTES=$(($OPTARG))
        else
            echo "Please, provide a number of bytes to package on."
            exit $EXIT_FAILURE
        fi
		;;
	S)  SAFETY_MODE_ENABLED=0
		;;
	d)  DELETE_RAW_FILES=0
		echo "Do not delete raw files."
		;;
    esac
done
shift $((OPTIND-1)) #ensure to consume the leftovers
# --------------------------------------------------------------------
#parsing end.

safety_mode_check ()
{
	if [[ $SAFETY_MODE_ENABLED -eq 1 ]]; then
		[ $VERBOSE -eq 1 ] && echo "[DEBUG] safety mode enabled."
	  	HOW_MANY_DIRS=`$S3CMD ls ${S3_PATH%/}/ | grep " DIR " | wc -l`
	  	if [[ $HOW_MANY_DIRS -gt 0 ]]; then
	  		echo "SAFETY MODE detected $HOW_MANY_DIRS directory(ies) in the given s3 path. For security reasons, exiting now."
	  		echo "If you ARE sure to package with directory, use -S option."
	  		exit $EXIT_FAILURE
	  	fi
	fi
}

# zero whether not nuggeted, non-zero for nuggeted.
check_already_nuggeted ()
{
	[ $VERBOSE -eq 1 ] && echo "[DEBUG] checking if there are nuggets in directory ${S3_PATH%/} ..."
  	ALREADY_NUGGETED=`$S3CMD ls ${S3_PATH%/}/_NUGGETED | wc -l`
  	[ $VERBOSE -eq 1 ] && echo "[DEBUG] Check result is: $ALREADY_NUGGETED"
  	[ $ALREADY_NUGGETED -eq 0 ]
}

nuggets()
{
	TOTAL_SIZE_BYTES=`$S3CMD du $S3_PATH | grep -o "[0-9]* "`
	NB_FILES=`$S3CMD ls ${S3_PATH%/}/ | wc -l`
	NB_SPLITS=`$PYTHON number_splits.py $NB_FILES $TOTAL_SIZE_BYTES $BLOCK_SIZE_BYTES`

	if [[ $NB_SPLITS -gt 0 ]]; then
		launch_hadoop_job
	else
		echo "Not enough bytes to crunch."
	fi
}

wipeout_workspace_directory()
{
	echo "Dropping nuggeted files..."
	echo -n $NUGGETED_BASENAME_FILES_LIST | xargs -d ' ' -r -P10 -I{} $S3CMD rm ${S3_PATH%/}/nugget-{}-$(printf '0x%x' $NUGGETS_TIMESTAMP).gz
	$S3CMD rm --recursive --quiet ${S3_PATH%/}/nuggeted	
}

launch_hadoop_job()
{
	echo "Listing $NB_FILES raw files..."

	$S3CMD ls ${S3_PATH%/}/
	echo $TOTAL_SIZE_BYTES | awk '{ size_in_mb = $1 / 1024 / 1024 ; print "Total size: " size_in_mb " MB" }'

	RAW_FILES_LIST=`$S3CMD ls ${S3_PATH%/}/ | grep -o "s3.*$"`

	echo "Launching job with $NB_SPLITS reduce(s) for path '${S3_PATH%/}'"
	EXISTING_OR_CREATE_CLUSTER=""
	if [[ $JOB_FLOW_IDENTIFIER ]]; then
		[ $VERBOSE -eq 1 ] && echo "[DEBUG] reusing an existing EMR cluster with id $JOB_FLOW_IDENTIFIER"
		EXISTING_OR_CREATE_CLUSTER="-j $JOB_FLOW_IDENTIFIER"
	else
		[ $VERBOSE -eq 1 ] && echo "[DEBUG] creating a new EMR cluster with id $NB_EC2_INSTANCES instances of type $EC2_INSTANCE_TYPE"
		EXISTING_OR_CREATE_CLUSTER="--create --ami-version 3.1.0 --name Cluster_for_nuggeting_files --num-instances $NB_EC2_INSTANCES --instance-type $EC2_INSTANCE_TYPE  --master-instance-type m1.medium --key-pair net2people --tag Name=Hadoop-for-nuggets"
	fi

	CONTAINS_DIRS=`$S3CMD ls ${S3_PATH%/}/ | grep " DIR " | wc -l`
	INPUT=${S3_PATH%/}
	if [[ $CONTAINS_DIRS -gt 0 ]]; then
		INPUT="${S3_PATH%/}/**/*"
	fi

	#Launch MapReduce job and wait execution
	$ELASTIC_MR $EXISTING_OR_CREATE_CLUSTER --stream --arg -Dmapreduce.output.fileoutputformat.compress=true --arg -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec --arg -Dmapreduce.job.reduces=$NB_SPLITS --step-name "Nuggeting files $NOW" --mapper /bin/cat --reducer /bin/cat --input $INPUT --output ${S3_PATH%/}/nuggeted --wait-for-steps
	RETVAL=$?

	if [[ $RETVAL -eq 0 ]]; then 
		
		#$ELASTIC_MR -j $JOB_FLOW_IDENTIFIER 
		#Checks whether job has not failed
		MAPREDUCE_SUCCESS=`$S3CMD ls ${S3_PATH%/}/nuggeted/_SUCCESS | wc -l`
		if [[ $MAPREDUCE_SUCCESS -eq 1 ]]; then
			
			echo "[OK] $NB_FILES files have been nuggeted in $NB_SPLITS parts."
			echo "Moving $NB_SPLITS nuggeted files to ${S3_PATH%/} ..."

			NUGGETS_TIMESTAMP=`date +%s`

			NUGGETED_BASENAME_FILES_LIST=`$S3CMD ls ${S3_PATH%/}/nuggeted/ | grep -o "part-[0-9]*" | xargs -r -I{} basename {}`

			NB_MOVED_FILES=`echo -n $NUGGETED_BASENAME_FILES_LIST | xargs -d ' ' -r -P10 -I{} $S3CMD mv ${S3_PATH%/}/nuggeted/{}.gz ${S3_PATH%/}/nugget-{}-$(printf '0x%x' $NUGGETS_TIMESTAMP).gz | wc -l`

			if [[ $NB_MOVED_FILES -eq  $NB_SPLITS ]]; then

				#Marks this repository as NUGGETED
				$S3CMD --quiet mv ${S3_PATH%/}/nuggeted/_SUCCESS  ${S3_PATH%/}/_NUGGETED

				if [[ $DELETE_RAW_FILES -eq 1 ]]; then
					echo "Deleting raw files..."
					S3CMD_RM_OPTIONS="rm"
					if [[ $CONTAINS_DIRS -gt 0 ]]; then
						S3CMD_RM_OPTIONS="rm --recursive"
					fi
					echo $RAW_FILES_LIST | xargs -d ' ' -r -P10 -I{} $S3CMD $S3CMD_RM_OPTIONS {}
				fi

				echo "Cleaning workspace..."
				$S3CMD rm --recursive --quiet ${S3_PATH%/}/nuggeted
				
				echo "End."
				RETVAL=$EXIT_SUCCESS
			else
				echo "[FAILURE] Something got wrong... Expected to move $NB_SPLITS files, Moved $NB_MOVED_FILES instead."
				wipeout_workspace_directory
				
				RETVAL=$EXIT_FAILURE
			fi
		else
			echo "[FAILURE] MapReduce Job has failed. "
			wipeout_workspace_directory

			RETVAL=$EXIT_FAILURE
		fi	
	else
		echo "Unable to launch the job. EMR tools has failed."
		RETVAL=$EXIT_FAILURE
	fi
}


S3CMD=`which s3cmd`
PYTHON=`which python`
ELASTIC_MR=`which elastic-mapreduce`

[ $VERBOSE -eq 1 ] && echo "[DEBUG] s3cmd tool path is $S3CMD"
[ $VERBOSE -eq 1 ] && echo "[DEBUG] ptyhon tool path is $PYTHON"
[ $VERBOSE -eq 1 ] && echo "[DEBUG] elastic-mapreduce tool path is $ELASTIC_MR"

if [[ ! $S3CMD ]]; then echo "[ERROR] s3cmd is not installed (trying to execute: $S3CMD). Please, get s3cmd tool on http://s3tools.org and ensure it is in your path!"; exit 1; fi
if [[ ! $PYTHON ]]; then echo "[ERROR] python is not installed (trying to execute: $PYTHON). Please, get python on http://python.org and ensure it is in your path!"; exit 1; fi
if [[ ! $ELASTIC_MR ]]; then echo "[ERROR] elastic-mapreduce is not installed (trying to execute: $ELASTIC_MR). Please, get elastic-mapreduce tool on http://aws.amazon.com/developertools/2264 and ensure it is in your path!"; exit 1; fi

NOW=$(date +"%d-%m-%Y_%H%M%S")

if [[ ! $JOB_FLOW_IDENTIFIER ]] && [[ $CREATE_CLUSTER -eq 0 ]]; then 
    echo "The job flow identifier must be provided or specify -c option (create cluster)." 
    exit $EXIT_FAILURE
fi

#main program begins below
S3_PATH="$@"
if [[ $S3_PATH ]]; then

	safety_mode_check
	check_already_nuggeted
	RETVAL=$?
	if [[ $RETVAL -eq 0 ]]; then
		nuggets
	else
		echo "No operation needed."
		RETVAL=$EXIT_SUCCESS
	fi
else
	echo "Please, provide the S3 path."
	RETVAL=$EXIT_FAILURE
fi
#--------------------------------------------------------------------
exit $RETVAL
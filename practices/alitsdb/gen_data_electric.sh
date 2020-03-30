#!/usr/bin/env bash

while [ -n "$1" ]
do
  case "$1" in
    --help)
        echo "
        --help              Show this help
        --userType          0,1
        --userCount         user count.
        --dataSet           generateDataSet1 or generateDataSet2.
        --lineCountPerUser
        --fileCount         file numbers.
        --startDate         startDate.
        --startTimestamp    startTimestamp.
        --output            where do you want to generate(directory), eg: /disk1/,/disk2"
        exit
        ;;
    --userType)
        _user_type=$2
        shift
        ;;
    --dataSet)
        _data_set=$2
        shift
        ;;
    --userCount)
        _user_count=$2
        shift
        ;;
    --lineCountPerUser)
        _line_count_per_user=$2
        shift
        ;;
    --fileCount)
        _file_count=$2
        shift
        ;;
    --startDate)
        _start_date=$2
        shift
        ;;
    --startTimestamp)
        _start_timestamp=$2
        shift
        ;;
    --output)
        _output=$2
        shift
        ;;
    *)
        echo "$1 is not an option"
        exit
        ;;
  esac
  shift
done

if [[ -z "$_output" ]]; then
    _output="./datas"
fi
echo "The path of data:$_output"

IFS=', ' read -r -a array <<< "$_output"
dir_count="${#array[@]}"

user_count_per_dir=`expr ${_user_count} / ${dir_count}`
echo "UserCount per directory: $user_count_per_dir"

file_count_per_dir=`expr ${_file_count} / ${dir_count}`

start_user_id=0

for outpt_current in "${array[@]}"
do
    start_user_id=`expr ${start_user_id} + ${user_count_per_dir}`

    if [ ${_data_set} = 'generateDataSet1' ]; then
      nohup java -jar test-data-generate-1.0-SNAPSHOT.jar ${_data_set} --userCount ${user_count_per_dir} --userType ${_user_type} --startUserId ${start_user_id} --startDate ${_start_date} --lineCountPerUser ${_line_count_per_user} --maxThreadCount ${file_count_per_dir} --fileCount ${file_count_per_dir} --output ${outpt_current} --aliTSDB --aliMetric electric >> /tmp/gen.log 2>>  /tmp/gen.log &
    else
      nohup java -jar test-data-generate-1.0-SNAPSHOT.jar ${_data_set} --userCount ${user_count_per_dir} --userType ${_user_type} --startUserId ${start_user_id} --startTimestamp ${_start_timestamp} --lineCountPerUser ${_line_count_per_user} --maxThreadCount ${file_count_per_dir} --fileCount ${file_count_per_dir} --output ${outpt_current} --aliTSDB --aliMetric electric2 >> /tmp/gen.log 2>>  /tmp/gen.log &
    fi
done


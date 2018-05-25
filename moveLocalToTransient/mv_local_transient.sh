#!/bin/bash
export PATH=$PATH

kinit -kt /home/usr_engineer/usr_engineer.keytab usr_engineer

fMOVE() {
	PROBES=$(echo $i | cut -c 33-35)
	PROBESR=$(echo $i | cut -c 37-39)

	YEARS=$(echo $i | cut -c 42-45)
	MONTHS=$(echo $i | cut -c 46-47)
	DAYS=$(echo $i | cut -c 48-49)

	YEARSR=$(echo $i | cut -c 43-46)
	MONTHSR=$(echo $i | cut -c 47-48)
	DAYSR=$(echo $i | cut -c 49-50)

	if [ $PROBES != "rob" ]; then
		echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -mkdir -p /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS"
		hadoop fs -mkdir -p /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS
		echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -moveFromLocal $i /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS"
		hadoop fs -moveFromLocal $i /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS
	else
		echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
		echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -moveFromLocal $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -moveFromLocal $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
	fi
}

for i in $(find /data/disks*/url_log | grep -v .ctrl | grep $1);do
	fMOVE
	echo "I Love Meme"
done&
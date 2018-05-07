#!/bin/bash
export PATH=$PATH

kinit -kt /home/usr_engineer/usr_engineer.keytab usr_engineer

fMOVE() {
	PROBES=$(echo $i | cut -c 55-57)
	PROBESR=$(echo $i | cut -c 56-58)

	YEARS=$(echo $i | cut -c 64-67)
	MONTHS=$(echo $i | cut -c 68-69)
	DAYS=$(echo $i | cut -c 70-71)

	YEARSR=$(echo $i | cut -c 62-65)
	MONTHSR=$(echo $i | cut -c 66-67)
	DAYSR=$(echo $i | cut -c 68-69)

	if [ $PROBES == "egb" ]; then
		echo "hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
		echo "hadoop fs -mv $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -mv $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
	elif [ $PROBES == "ekt" ]; then
		echo "hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -mkdir -p /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
		echo "hadoop fs -mv $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR"
		hadoop fs -mv $i /data/transient/url_${PROBESR}_${YEARSR}${MONTHSR}/ds=$YEARSR-$MONTHSR-$DAYSR
	else
		echo "hadoop fs -mkdir -p /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS"
		hadoop fs -mkdir -p /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS
		echo "hadoop fs -mv $i /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS"
		hadoop fs -mv $i /data/transient/url_${PROBES}_${YEARS}${MONTHS}/ds=$YEARS-$MONTHS-$DAYS
	fi
}

for i in $(hadoop fs -ls /data/landing/mrs/url_log/*/*/* | grep $1 | awk '{print $8}');do
	fMOVE
done&
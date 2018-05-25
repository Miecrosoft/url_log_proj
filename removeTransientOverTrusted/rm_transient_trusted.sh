#!/bin/bash
export PATH=$PATH

kinit -kt /home/usr_engineer/usr_engineer.keytab usr_engineer

fREMOVE() {
	sedTrusted=$(echo $i | sed 's/transient/trusted/g')
	countFile=$(hadoop fs -ls $sedTrusted | wc -l)
	echo "hadoop fs -test -e $sedTrusted"
	hadoop fs -test -e $sedTrusted
	if [ $? -eq 0 ] && [ $countFile -gt 300 ]; then
		echo "=============================="
		echo "$? && $countFile > 300"
		echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -rm -R -skipTrash $i"
		hadoop fs -rm -R -skipTrash $i
		echo -e "============================== \n"
	else
		echo -e "`date +%Y-%m-%dT%H:%M:%S` Path $i tidak ada di trusted atau countFile <= 300 \n"
	fi
}

for i in $(hadoop fs -ls /data/transient/url_$1_201802/ | awk '{print $8}');do
	fREMOVE
done

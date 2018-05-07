#!/bin/bash
export PATH=$PATH
# set -x

DYes=$(date +"%Y%m%d" --date='-1 day')
Years=$(echo $DYes | cut -c 1-4)
Months=$(echo $DYes | cut -c 5-6)
Days=$(echo $DYes | cut -c 7-8)

MAINS=/home/usr_engineer/url_prj
HDFS=/data/landing/mrs/url_log

KBL=/data/disks1/url_log/
RKT=/data/disks2/url_log/
GIN=/data/disks3/url_log/
KLM=/data/disks4/url_log/
MTR=/data/disks5/url_log/
KUP=/data/disks6/url_log/
KTB=/data/disks7/url_log/

kinit -kt /home/usr_engineer/usr_engineer.keytab usr_engineer

hadoop fs -mkdir -p $HDFS/kbl-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/kbl-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/kbl-np3p/$Years/$Months
hadoop fs -mkdir -p $HDFS/rkt-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/rkt-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/rkt-np3p/$Years/$Months
hadoop fs -mkdir -p $HDFS/gin-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/klm-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/mtr-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/kup-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/ktbp1/$Years/$Months
hadoop fs -mkdir -p $HDFS/ktbp2/$Years/$Months

fKBL1() {
    hadoop fs -put $i $HDFS/kbl-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KBL]`' > $MAINS/logs/kbl1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kbl1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kbl1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kbl1
    Sizes_Tele=$(hadoop fs -ls $HDFS/kbl-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kbl1
    cat $MAINS/logs/kbl1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKBL2() {
    hadoop fs -put $i $HDFS/kbl-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KBL]`' > $MAINS/logs/kbl2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kbl2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kbl2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kbl2
    Sizes_Tele=$(hadoop fs -ls $HDFS/kbl-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kbl2
    cat $MAINS/logs/kbl2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKBL3() {
    hadoop fs -put $i $HDFS/kbl-np3p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KBL]`' > $MAINS/logs/kbl3
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kbl3
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kbl3
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kbl3
    Sizes_Tele=$(hadoop fs -ls $HDFS/kbl-np3p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kbl3
    cat $MAINS/logs/kbl3 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fRKT1() {
    hadoop fs -put $i $HDFS/rkt-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[RKT]`' > $MAINS/logs/rkt1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/rkt1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/rkt1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/rkt1
    Sizes_Tele=$(hadoop fs -ls $HDFS/rkt-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/rkt1
    cat $MAINS/logs/rkt1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fRKT2() {
    hadoop fs -put $i $HDFS/rkt-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[RKT]`' > $MAINS/logs/rkt2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/rkt2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/rkt2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/rkt2
    Sizes_Tele=$(hadoop fs -ls $HDFS/rkt-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/rkt2
    cat $MAINS/logs/rkt2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fRKT3() {
    hadoop fs -put $i $HDFS/rkt-np3p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[RKT]`' > $MAINS/logs/rkt3
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/rkt3
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/rkt3
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/rkt3
    Sizes_Tele=$(hadoop fs -ls $HDFS/rkt-np3p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/rkt3
    cat $MAINS/logs/rkt3 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fGIN1() {
    hadoop fs -put $i $HDFS/gin-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[GIN]`' > $MAINS/logs/gin1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/gin1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/gin1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/gin1
    Sizes_Tele=$(hadoop fs -ls $HDFS/gin-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/gin1
    cat $MAINS/logs/gin1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKLM1() {
    hadoop fs -put $i $HDFS/klm-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KLM]`' > $MAINS/logs/klm1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/klm1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/klm1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/klm1
    Sizes_Tele=$(hadoop fs -ls $HDFS/klm-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/klm1
    cat $MAINS/logs/klm1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fMTR1() {
    hadoop fs -put $i $HDFS/mtr-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[MTR]`' > $MAINS/logs/mtr1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/mtr1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/mtr1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/mtr1
    Sizes_Tele=$(hadoop fs -ls $HDFS/mtr-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/mtr1
    cat $MAINS/logs/mtr1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKUP1() {
    hadoop fs -put $i $HDFS/kup-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KUP]`' > $MAINS/logs/kup1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kup1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kup1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kup1
    Sizes_Tele=$(hadoop fs -ls $HDFS/kup-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kup1
    cat $MAINS/logs/kup1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKTB1() {
    hadoop fs -put $i $HDFS/ktb1p1/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KTB]`' > $MAINS/logs/ktb1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/ktb1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/ktb1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/ktb1
    Sizes_Tele=$(hadoop fs -ls $HDFS/ktb1p1/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/ktb1
    cat $MAINS/logs/ktb1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKTB2() {
    hadoop fs -put $i $HDFS/ktb2p1/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
        echo '`[KTB]`' > $MAINS/logs/ktb2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/ktb2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/ktb2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/ktb2
    Sizes_Tele=$(hadoop fs -ls $HDFS/ktb2p1/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/ktb2
    cat $MAINS/logs/ktb2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}


cd $KBL
for i in $(ls -lh $KBL | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep kbl-np1p
    if [ $? -eq 0 ]; then
        fKBL1
    else
        echo $i | grep kbl-np2p
        if [ $? -eq 0 ]; then
            fKBL2
        else
        	echo $i | grep kbl-np3p
        	if [ $? -eq 0 ]; then
        		fKBL3
        	fi
        fi
    fi
done&

cd $RKT
for i in $(ls -lh $RKT | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep rkt-np1p
    if [ $? -eq 0 ]; then
        fRKT1
    else
        echo $i | grep rkt-np2p
        if [ $? -eq 0 ]; then
            fRKT2
        else
        	echo $i | grep rkt-np3p
        	if [ $? -eq 0 ]; then
        		fRKT3
        	fi
        fi
    fi
done&

cd $GIN
for i in $(ls -lh $GIN | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep gin-np1p
    if [ $? -eq 0 ]; then
        fGIN1
    fi
done&

cd $KLM
for i in $(ls -lh $KLM | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep klm-np1p
    if [ $? -eq 0 ]; then
        fKLM1
    fi
done&

cd $MTR
for i in $(ls -lh $MTR | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep mtr-np1p
    if [ $? -eq 0 ]; then
        fMTR1
    fi
done&

cd $KUP
for i in $(ls -lh $KUP | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep kup-np1p
    if [ $? -eq 0 ]; then
        fKUP1
    fi
done&

cd $KTB
for i in $(ls -lh $KTB | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep ktbp1
    if [ $? -eq 0 ]; then
        fKTB1
    else
    	echo $i | grep ktbp2
    	if [ $? -eq 0 ]; then
    		fKTB2
    	fi
    fi
done&
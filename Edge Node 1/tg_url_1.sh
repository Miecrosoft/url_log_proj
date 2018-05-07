#!/bin/bash
export PATH=$PATH
# set -x

DYes=$(date +"%Y%m%d" --date='-1 day')
Years=$(echo $DYes | cut -c 1-4)
Months=$(echo $DYes | cut -c 5-6)
Days=$(echo $DYes | cut -c 7-8)

MAINS=/home/usr_engineer/url_prj
HDFS=/data/landing/mrs/url_log

CKA=/data/disks1/url_log/
JT2=/data/disks2/url_log/
BKS=/data/disks3/url_log/
SLP=/data/disks4/url_log/
KBB=/data/disks5/url_log/
BOO=/data/disks6/url_log/
GBL=/data/disks7/url_log/

kinit -kt /home/usr_engineer/usr_engineer.keytab usr_engineer

hadoop fs -mkdir -p $HDFS/cka-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/cka-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/jt2-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/jt2-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/bks-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/bks-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/slp-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/slp-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/kbb-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/kbb-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/boo-np1p/$Years/$Months
hadoop fs -mkdir -p $HDFS/boo-np2p/$Years/$Months
hadoop fs -mkdir -p $HDFS/gblp1/$Years/$Months

fCKA1() {
    hadoop fs -put $i $HDFS/cka-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[CKA]`' > $MAINS/logs/cka1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/cka1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/cka1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/cka1
    Sizes_Tele=$(hadoop fs -ls $HDFS/cka-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/cka1
    cat $MAINS/logs/cka1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fCKA2() {
    hadoop fs -put $i $HDFS/cka-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[CKA]`' > $MAINS/logs/cka2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/cka2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/cka2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/cka2
    Sizes_Tele=$(hadoop fs -ls $HDFS/cka-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/cka2
    cat $MAINS/logs/cka2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fJT21() {
    hadoop fs -put $i $HDFS/jt2-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[JT2]`' > $MAINS/logs/jt21
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/jt21
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/jt21
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/jt21
    Sizes_Tele=$(hadoop fs -ls $HDFS/jt2-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/jt21
    cat $MAINS/logs/jt21 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fJT22() {
    hadoop fs -put $i $HDFS/jt2-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[JT2]`' > $MAINS/logs/jt22
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/jt22
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/jt22
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/jt22
    Sizes_Tele=$(hadoop fs -ls $HDFS/jt2-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/jt22
    cat $MAINS/logs/jt22 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fBKS1() {
    hadoop fs -put $i $HDFS/bks-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[BKS]`' > $MAINS/logs/bks1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/bks1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/bks1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/bks1
    Sizes_Tele=$(hadoop fs -ls $HDFS/bks-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/bks1
    cat $MAINS/logs/bks1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fBKS2() {
    hadoop fs -put $i $HDFS/bks-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[BKS]`' > $MAINS/logs/bks2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/bks2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/bks2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/bks2
    Sizes_Tele=$(hadoop fs -ls $HDFS/bks-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/bks2
    cat $MAINS/logs/bks2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fSLP1() {
    hadoop fs -put $i $HDFS/slp-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[SLP]`' > $MAINS/logs/slp1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/slp1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/slp1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/slp1
    Sizes_Tele=$(hadoop fs -ls $HDFS/slp-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/slp1
    cat $MAINS/logs/slp1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fSLP2() {
    hadoop fs -put $i $HDFS/slp-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[SLP]`' > $MAINS/logs/slp2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/slp2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/slp2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/slp2
    Sizes_Tele=$(hadoop fs -ls $HDFS/slp-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/slp2
    cat $MAINS/logs/slp2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKBB1() {
    hadoop fs -put $i $HDFS/kbb-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[KBB]`' > $MAINS/logs/kbb1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kbb1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kbb1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kbb1
    Sizes_Tele=$(hadoop fs -ls $HDFS/kbb-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kbb1
    cat $MAINS/logs/kbb1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fKBB2() {
    hadoop fs -put $i $HDFS/kbb-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[KBB]`' > $MAINS/logs/kbb2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/kbb2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/kbb2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/kbb2
    Sizes_Tele=$(hadoop fs -ls $HDFS/kbb-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/kbb2
    cat $MAINS/logs/kbb2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fBOO1() {
    hadoop fs -put $i $HDFS/boo-np1p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[BOO]`' > $MAINS/logs/boo1
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/boo1
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/boo1
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/boo1
    Sizes_Tele=$(hadoop fs -ls $HDFS/boo-np1p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/boo1
    cat $MAINS/logs/boo1 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fBOO2() {
    hadoop fs -put $i $HDFS/boo-np2p/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[BOO]`' > $MAINS/logs/boo2
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/boo2
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/boo2
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/boo2
    Sizes_Tele=$(hadoop fs -ls $HDFS/boo-np2p/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/boo2
    cat $MAINS/logs/boo2 | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}

fGBL() {
    hadoop fs -put $i $HDFS/gblp1/$Years/$Months
    sleep 1
    IFS=' '
    while read names rows sizes; do
    	echo '`[GBL]`' > $MAINS/logs/gbl
        echo "$names ($(awk 'BEGIN {printf "%.2f GB\n",'$sizes'/1073741824}'))" >> $MAINS/logs/gbl
        echo $rows | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta' >> $MAINS/logs/gbl
    done < $i.ctrl
    echo "[Hadoop Metadata]" >> $MAINS/logs/gbl
    Sizes_Tele=$(hadoop fs -ls $HDFS/gblp1/$Years/$Months/$i | awk '{print $5}')
    Sizes_Telec=$(awk 'BEGIN {printf "%.2f GB\n",'$Sizes_Tele'/1073741824}')
    echo "$i ($Sizes_Telec)" >> $MAINS/logs/gbl
    cat $MAINS/logs/gbl | telegram-send --stdin --format markdown
    rm -rf $i $i.ctrl
}


cd $CKA
for i in $(ls -lh $CKA | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep cka-np1p
    if [ $? -eq 0 ]; then
        fCKA1
    else
        echo $i | grep cka-np2p
        if [ $? -eq 0 ]; then
            fCKA2
        fi
    fi
done&

cd $JT2
for i in $(ls -lh $JT2 | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep jt2-np1p
    if [ $? -eq 0 ]; then
        fJT21
    else
        echo $i | grep jt2-np2p
        if [ $? -eq 0 ]; then
            fJT22
        fi
    fi
done&

cd $BKS
for i in $(ls -lh $BKS | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep bks-np1p
    if [ $? -eq 0 ]; then
        fBKS1
    else
        echo $i | grep bks-np2p
        if [ $? -eq 0 ]; then
            fBKS2
        fi
    fi
done&

cd $SLP
for i in $(ls -lh $SLP | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep slp-np1p
    if [ $? -eq 0 ]; then
        fSLP1
    else
        echo $i | grep slp-np2p
        if [ $? -eq 0 ]; then
            fSLP2
        fi
    fi
done&

cd $KBB
for i in $(ls -lh $KBB | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep kbb-np1p
    if [ $? -eq 0 ]; then
        fKBB1
    else
        echo $i | grep kbb-np2p
        if [ $? -eq 0 ]; then
            fKBB2
        fi
    fi
done&

cd $BOO
for i in $(ls -lh $BOO | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep boo-np1p
    if [ $? -eq 0 ]; then
        fBOO1
    else
        echo $i | grep boo-np2p
        if [ $? -eq 0 ]; then
            fBOO2
        fi
    fi
done&

cd $GBL
for i in $(ls -lh $GBL | grep -v .ctrl | grep url | grep $Years$Months$Days | awk '{print $9}');do
    echo $i | grep gblp1
    if [ $? -eq 0 ]; then
        fGBL
    fi
done&
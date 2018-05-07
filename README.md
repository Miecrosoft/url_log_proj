# url_log_proj

## Edge Node 1 and Edge Node 2

Edge Node 1 and Edge Node 2 are used for pushing data from **local** to **/data/landing** and also do send notifications to Telegram. They will be put to cron job so it could be running automatically.

## moveHDFSToTransient

This script is used to move data in HDFS (/data/landing) to **/data/transient** as a database folder.

## moveLocalToTransient

This is different with Edge Node 1 & 2 script. This script is used to move from **local*p to **/data/transient** directly. This script help me to skip **/data/landing** (Edge Node 1 and 2).

## moveTransientOverTrusted

This script will remove data in **/data/transient** if they are exist in **/data/trusted**. So, this script will help me for reducing usage of HDFS disk.
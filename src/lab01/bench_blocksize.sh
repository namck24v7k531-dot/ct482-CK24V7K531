#!/bin/bash
#!/usr/bin/env bash
# Đo thời gian hdfs dfs -put với nhiều kích thước block, ghi kết quả ra CSV
# Chạy bên trong container namenode: bash /src/lab01/bench_blocksize.sh /tmp/test_1gb.dat 3
FILE=${1:-/tmp/test_1gb.dat}
RUNS=${2:-3}
OUT=/src/lab01/ket_qua_blocksize.csv
DEST=/user/student/lab01/bench
hdfs dfs -mkdir -p $DEST
echo "block_mb,lan,giay,so_block" > $OUT
for MB in 32 64 128 256 512; do
  BS=$((MB * 1024 * 1024))
  for i in $(seq 1 $RUNS); do
    hdfs dfs -rm -f -skipTrash $DEST/test.dat > /dev/null 2>&1
    T0=$(date +%s.%N)
    hdfs dfs -D dfs.blocksize=$BS -put $FILE $DEST/test.dat
    T1=$(date +%s.%N)
    NB=$(hdfs fsck $DEST/test.dat -blocks 2>/dev/null | grep -oP 'Total blocks \(validated\):\s*\K[0-9]+')
    echo "$MB,$i,$(awk -v a=$T0 -v b=$T1 'BEGIN{printf "%.2f", b-a}'),$NB" | tee -a $OUT
  done
done
hdfs dfs -rm -r -f -skipTrash $DEST > /dev/null
echo "--- trung bình (giây) theo kích thước block ---"
awk -F, 'NR>1 {s[$1]+=$3; n[$1]++; nb[$1]=$4} END {for (k in s) printf "%s MB: %.2f s (%d block)\n", k, s[k]/n[k], nb[k]}' $OUT | sort -n
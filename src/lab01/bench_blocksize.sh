
#!/usr/bin/env bash

# Do thoi gian hdfs dfs -put voi nhieu kich thuoc block, ghi ket qua
# Chay ben trong container namenode

FILE=${1:-/tmp/test_1gb.dat}
RUNS=${2:-3}

OUT=/src/lab01/ket_qua_blocksize.csv
DEST=/user/student/lab01/bench

mkdir -p "$(dirname "$OUT")"
hdfs dfs -mkdir -p "$DEST"

echo "block_mb,lan,giay,so_block" > "$OUT"

for MB in 32 64 128 256 512; do
    BS=$((MB * 1024 * 1024))

    for i in $(seq 1 "$RUNS"); do
        hdfs dfs -rm -f -skipTrash "$DEST/test.dat" >/dev/null 2>&1 || true

        T0=$(date +%s.%N)

        hdfs dfs -D dfs.blocksize=$BS -put "$FILE" "$DEST/test.dat"

        T1=$(date +%s.%N)

        TIME=$(awk -v a="$T0" -v b="$T1" 'BEGIN {printf "%.3f", b-a}')

        BLOCKS=$(hdfs fsck "$DEST/test.dat" -files -blocks 2>/dev/null \
            | grep -o 'blk_[0-9]*' | sort -u | wc -l | tr -d ' ')

        echo "$MB,$i,$TIME,$BLOCKS" >> "$OUT"

        echo "Block size: ${MB}MB | Lan: $i | Thoi gian: ${TIME}s | So block: $BLOCKS"
    done
done

echo "Da hoan thanh benchmark!"
echo "Ket qua duoc luu tai: $OUT"

#!/bin/sh

echo "--- 🔼🔼🔼🔼🔼🔼 ---"
grep -v '^#' /app/lista.txt | xargs -I {} rclone copy  /app/cold_storage/data_mercados "midrive:{}"
echo "--- 🏁🏁🏁  Donete 🏁🏁🏁  ---"
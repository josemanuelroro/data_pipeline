#!/bin/sh

echo "--- 🔽🔽🔽🔽🔽🔽 ---"
grep -v '^#' /config/rclone/lista.txt | xargs -I {} rclone copy "midrive:{}" /data --verbose
echo "--- 🏁🏁🏁 Donete 🏁🏁🏁  ---"
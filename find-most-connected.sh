cat text_sorted.txt | awk '{print $1}' | uniq -c | head -n 100000 | sort -r | less

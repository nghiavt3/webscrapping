# Trong file run_all_spiders.sh/.bat
scrapy crawl event_cat -o news.json -L WARNING &
scrapy crawl event_table_scraper -o news.json -o news.json -L WARNING &
wait # Đợi cả hai lệnh hoàn thành
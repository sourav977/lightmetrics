# lightmetrics
remove tombstone documents from cloudant backup file which is downloaded using couchbackup cli tool and the upload it to IBM COS bucket

## create virtual env
```
virtualenv --python=python3.9 venv
. venv/bin/activate
```

## install required library
```
pip3 install -r requirements.txt
```

## run python file
```
python3 remove-tombstone-then-upload-cos.py
```

## expected output
```
% python3 remove-tombstone-then-upload-cos.py
Filtered data written to dbname_tombstonefree.txt
Comparison Date: 2023-05-20
Filtered data written to dbname_tombstonefree_filtered_by_date.txt
Starting multi-part upload for dbname_backup_2023_12_05 to bucket: dbname-backup-sourav

Uploading to dbname_backup_2023_12_05 (part 1 of 1)
Upload for dbname_backup_2023_12_05 Complete!
```
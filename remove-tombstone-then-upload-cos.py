import json
import math
import os
import ibm_boto3
from ibm_botocore.client import Config, ClientError
from datetime import datetime, timezone

# working
# Constants for IBM COS values
COS_ENDPOINT = "https://s3.us.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = "<apikey>" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
COS_INSTANCE_CRN = "<crn..>" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"

# client to connect IBM COS instance
cos_client = ibm_boto3.client("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

def filter_deleted(json_data):
    return [block for block in json_data if not block.get("_deleted", False)]


def filter_date_save(json_data, comparison_date):
    comparison_datetime = datetime.strptime(comparison_date, '%Y-%m-%d')
    return [block for block in json_data if ("docUpdatedAt" in block and datetime.strptime(block["docUpdatedAt"], '%Y-%m-%dT%H:%M:%S.%fZ') > comparison_datetime)]


def filter_operation(input_file, output_file, comparison_date):
    try:
        with open(output_file, "w") as ofile:
            with open(input_file, "r") as ifile:
                input_lines = ifile.readlines()
                for input_line in input_lines:
                    if len(comparison_date) != 0:
                        output_line = filter_date_save(json.loads(input_line), comparison_date)
                    else:
                        output_line = filter_deleted(json.loads(input_line))
                    if len(output_line) == 0:
                        continue
                    output_line_json = json.dumps(output_line)
                    ofile.writelines(str(output_line_json) + "\n")

        print(f"Filtered data written to {output_file}")

    except FileNotFoundError:
        print("File not found. Please check the file path.")
    except json.JSONDecodeError:
        print("Error decoding JSON. Please ensure the file contains valid JSON.")
    except Exception as e:
        print(f"An error occurred: {e}")    


# upload to IBM COS bucket
def multi_part_upload_manual(bucket_name, item_name, file_path):
    try:
        print("Starting multi-part upload for {0} to bucket: {1}\n".format(item_name, bucket_name))
        # initiate the multi-part upload
        mp = cos_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=item_name
        )
        upload_id = mp["UploadId"]
        # min 20MB part size
        part_size = 1024 * 1024 * 20
        file_size = os.stat(file_path).st_size
        part_count = int(math.ceil(file_size / float(part_size)))
        data_packs = []
        position = 0
        part_num = 0
        # begin uploading the parts
        with open(file_path, "rb") as file:
            for i in range(part_count):
                part_num = i + 1
                part_size = min(part_size, (file_size - position))
                print("Uploading to {0} (part {1} of {2})".format(item_name, part_num, part_count))
                file_data = file.read(part_size)
                mp_part = cos_client.upload_part(
                    Bucket=bucket_name,
                    Key=item_name,
                    PartNumber=part_num,
                    Body=file_data,
                    ContentLength=part_size,
                    UploadId=upload_id
                )

                data_packs.append({
                    "ETag":mp_part["ETag"],
                    "PartNumber":part_num
                })

                position += part_size

        # complete upload
        cos_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=item_name,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": data_packs
            }
        )
        print("Upload for {0} Complete!\n".format(item_name))
    except ClientError as be:
        # abort the upload
        cos_client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=item_name,
            UploadId=upload_id
        )
        print("Multi-part upload aborted for {0}\n".format(item_name))
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to complete multi-part upload: {0}".format(e))


#first delete tombstone documents, then filter by date, then upload to IBM COS
def main(input_file, output_file, comparison_date):
    # first remove the tombstone documents from backup file
    filter_operation(input_file, output_file,"")
    file_name, file_extension = os.path.splitext(output_file_path)

    filtered_by_date_file_path = f'{file_name}_filtered_by_date.txt'
    # now use tombstone free file to filter the documents by date
    print("Comparison Date:", comparison_date)
    filter_operation(output_file, filtered_by_date_file_path, comparison_date)

    # bucket name, item name, file path
    multi_part_upload_manual("dbname-backup-sourav", "dbname_backup_2023_12_05", filtered_by_date_file_path)


if __name__ == "__main__":
    input_file_path = 'dbname_backup.txt' # name of backup file downloaded and stored using couchbackup cli tool
    output_file_path = 'dbname_tombstonefree.txt' # file to store tombstone free documents

    comparison_date = "2023-05-20" # date after which you want the documents

    #first delete tombstone documents, then filter by date, then upload to IBM COS 
    main(input_file_path, output_file_path, comparison_date)



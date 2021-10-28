#codeupdate123tes26123CAORtestanudeep
import json
import boto3
pipeline = boto3.client('codepipeline')
"""from PyPDF4 import PdfFileReader, PdfFileWriter
import os
from io import BytesIO
from datetime import date, time, datetime
import psycopg2
import uuid
import logging
import warnings
warnings.filterwarnings("ignore")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def rds_connection():
    rds_host = 'bh-dcap.cysf64wiobfj.us-east-1.rds.amazonaws.com'
    name = "bhadmin"
    password = "Baker123"
    db_name = "postgres"
    try:
        conn_string = "host=%s user=%s password=%s dbname=%s" % (rds_host, name, password, db_name)
        connection = psycopg2.connect(conn_string)
        logger.info("RDS server information :==> " + str(connection.get_dsn_parameters()))
    except (Exception, psycopg2.Error) as error:
        logger.info("Error while connecting to PostgreSQL"+ error)
        connection.close()
        logger.info("RDS connection is closed")
    return connection"""
def lambda_handler(event, context):
    # bucket = event['bucket_name']
    # key = event['file_name']
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = event['Records'][0]['s3']['object']['key']
    print('text sample')

    response = pipeline.put_job_success_result(
        jobId=event['CodePipeline.job']['id']
    )
    return response

    """# RDS Connection
    conn = rds_connection()
    cursor = conn.cursor()
    table_name = 'vlm_spilt_pdf'
    cursor.execute("select * from information_schema.tables where table_name=%s", ('vlm_spilt_pdf',))
    if bool(cursor.rowcount) == False:
        create_table_query = CREATE TABLE {} (ID VARCHAR(64) PRIMARY KEY,
                            FILENAME TEXT NOT NULL, PAGECOUNT INT,
                            DATE timestamp NOT NULL, SPLIT_FILES_PATH TEXT,
                            ARCHIVE_FILES_PATH TEXT, SPLIT_STATUS TEXT NOT NULL
                            );.format(table_name)
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Table created successfully in PostgreSQL ")
    else:
        logger.info("Table exists already ")
    query = "INSERT INTO vlm_spilt_pdf (ID, FILENAME, PAGECOUNT, DATE, SPLIT_FILES_PATH, ARCHIVE_FILES_PATH, SPLIT_STATUS) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        # bucket = event['bucket_name']
        # key = event['file_name']
        dirpath = os.path.dirname(key)
        logger.info("Connecting to " + bucket + " S3 bucket")
        s3 = boto3.resource('s3')
        client = boto3.client('s3')
        my_bucket = s3.Bucket(bucket)
        logger.info("Date time folder path")
        filedt = datetime.utcnow()
        fileDate = filedt.strftime('%Y-%m-%d-%H-%M-%S')
        logger.info("Copying & deleting of VLM pdf files from landing zone to archive vlm folder")
        for my_bucket_object in my_bucket.objects.filter(Prefix=dirpath):
            filepath = str(my_bucket_object.key)
            filename = filepath.split("/")[-1]
            print(filename)
            if filepath.endswith(".pdf"):
                targetkey = "landing-zone/archive-vlm/" + fileDate + "/" + filename
                response1 = client.copy_object(Bucket=bucket, Key=targetkey,
                                               CopySource={'Bucket': bucket, 'Key': filepath})
                logger.info("Copying of VLM pdfs is done. Filename is" + filename)
                logger.info("Splitting pdf file has been started.. Filename is:" + filename)
                result = client.get_object(Bucket=bucket, Key=filepath)
                body = result['Body'].read()
                f = PdfFileReader(BytesIO(body))
                for i in range(f.numPages):
                    f = PdfFileReader(BytesIO(body))
                    output = PdfFileWriter()
                    output.addPage(f.getPage(i))
                    page_num = str(i + 1)
                    tmp_path = str("/tmp/" + filename + "_page_" + page_num + ".pdf")
                    with open(tmp_path, "wb") as outputStream:
                        output.write(outputStream)
                    split_key_object = str(
                        "processing-zone/split-pdfs/" + fileDate + "/" + filename[:-4] + "_page_" + page_num + ".pdf")
                    my_bucket.upload_file(tmp_path, split_key_object)
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                        logger.info("Removed the file %s" % tmp_path)
                    else:
                        logger.info("Sorry, file %s does not exist." % tmp_path)
                logger.info("Splitting pdf file has been successfully finished. Filename is: " + filename)
                response2 = client.delete_object(Bucket=bucket, Key=filepath)
                logger.info("Delete of PO pdfs from trigger zone is done. Filename is" + filename)
                tbl = (str(uuid.uuid4().time), filename, f.numPages, datetime.utcnow(),
                       str("processing-zone/split-pdfs/" + fileDate), str("landing-zone/archive-vlm/" + fileDate),
                       "Success")
                cursor.execute(query, tbl)
                conn.commit()
            elif filepath.endswith(".json"):
                logger.info("No Json file is found. filename is" + filename)
            else:
                logger.info("No file found in trigger-folder")
        return {
            'statusCode': 200,
            'body': json.dumps('Successful..!!')
        }
    except Exception as e:
        logger.info("Splitting process failed " + str(repr(e)))
        tbl = (str(uuid.uuid4().time), filename, '', datetime.utcnow(), '', '', "Failure")
        cursor.execute(query, tbl)
        conn.commit()
        return {
            'statusCode': 500,
            'body': json.dumps('Splitting failed ..!!')
        }
    finally:
        if (conn):
            cursor.close()
            conn.close()
            logger.info("RDS connection is closed")"""

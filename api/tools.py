from io import BytesIO, StringIO
import csv
import zipfile

def convert_records_to_csv(records):
    sample = records[0]
    fieldnames = list(sample.keys())
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(records)
    return csv_buffer

def create_zipfile(files_to_zip):
    zip_buffer = BytesIO()
    zipfile_instance = zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED)
    for d in files_to_zip:
        zipfile_instance.writestr(
            d['name'],
            d['file'].getvalue()
        )
    zipfile_instance.close()
    return zip_buffer

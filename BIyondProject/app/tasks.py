from __future__ import absolute_import, unicode_literals
from celery import shared_task
import random
from .models import FileTable

CORRUPT = 1


def raise_corrupted_files_exception(corrupted_files):
    """
    Turn the corrupted files list into a string and then raise an exception
    :param corrupted_files: List of the files name that are corrupted
    """
    corrupted_files_str = [str(corrupted_files[i]) + ', ' for i in range(len(corrupted_files) - 1)]
    corrupted_files_str += corrupted_files[-1]

    raise Exception("The corrupted files are : {}".format(corrupted_files_str))


@shared_task
def load_files(files_list):
    """
    For each file in receive list, check if that file is corrupt if so append it corrupted_ files list
    otherwise save it in the database. At the end if a corrupted file has been found raise an exception.
    :param files_list: List of file names
    :return:
    """
    corrupted_files = []

    for file_name in files_list:
        # Random a number between 1 - 10
        is_corrupt = random.randint(1, 11)

        # Check if the number is 1 then the file is corrupt
        if is_corrupt == CORRUPT:
            corrupted_files.append(file_name)
        else:
            # Save the file name to the DB
            new_file = FileTable(name=file_name)
            new_file.save()

    if len(corrupted_files) > 0:
        raise_corrupted_files_exception(corrupted_files)

    print('All files have been added successfully to the DB')

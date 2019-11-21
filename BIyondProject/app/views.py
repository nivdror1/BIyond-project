from django.shortcuts import HttpResponse
from django.views import View
from .tasks import load_files
from django.views.decorators.csrf import csrf_exempt
import json

SECONDS_TO_WAIT = 1
FILES_LIST_ARGUMENT = "files_list"


def validate_content(content):
    """
    Validate the content of the request body - check if the are file names in the request body
    :param content: The request body
    :return: Boolean
    """
    if FILES_LIST_ARGUMENT in content:
        if len(content[FILES_LIST_ARGUMENT]) > 0:
            return True

    return False


@csrf_exempt
def etl_view(request):
    """
    A function view which checks if the request contain the file names if so calls the async function which load files
    otherwise
    :param request:
    :return:
    """
    if request.method == 'POST':
        body_unicode = request.body.decode('utf-8')
        content = json.loads(body_unicode)

        if validate_content(content):
            load_files.apply_async([content[FILES_LIST_ARGUMENT]], countdown=SECONDS_TO_WAIT)
            return HttpResponse("files are currently being loaded")

    return HttpResponse(status=400)
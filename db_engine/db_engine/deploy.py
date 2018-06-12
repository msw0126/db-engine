import codecs
import csv
import json

import os

import shutil
from django.http import HttpResponse

from F_SETTING import WORKING_DIRECTORY
from common.UTIL import auto_param




@auto_param
def export(request, project_id, component_id):

    component_directory = os.path.join( WORKING_DIRECTORY, project_id, component_id )
    if not os.path.exists( component_directory ):
        os.makedirs( component_directory )
    export_path = os.path.join(component_directory, "%s_%s_export_model.zip" % (project_id, component_id))

    z_file = open( export_path, 'rb' )
    data = z_file.read()
    z_file.close()
    response = HttpResponse( data, content_type='application/zip' )
    response['Content-Disposition'] = 'attachment;filename=%s_%s_ExportModel.zip' % ( project_id, component_id)

    return response
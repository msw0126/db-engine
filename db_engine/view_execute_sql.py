from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JNetworkError, Py4JJavaError

from common import ERRORS
import logging


if __name__ == "__main__":
    import sys, os, django
    sys.path.append(os.path.dirname(os.path.realpath(sys.argv[0])))
    os.environ['DJANGO_SETTINGS_MODULE'] = 'db_engine.settings'
    django.setup()
    # Task.objects.filter(project_id='222',component_id__in=['1','2']).update(relies=F("relies")-1)
    # print(connection.queries)
    # from executor.celery_tasks import component2_execute
    # component2_execute.delay(project_id='test_p_id',component_id='test_component_id')

    # Task.objects.update_or_create(
    #     project_id='test_p_id', component_id='test_component_id',
    #     defaults=dict(
    #         task_id='0',
    #         component_type='type',
    #         required_memory=None,
    #         required_container=None,
    #         required_cpu=None,
    #         task_status="PENDING",
    #         relies=5,
    #         record_time=datetime.datetime.now(),
    #         detail=None,
    #         start_time=None,
    #         end_time=None
    #     ),
    # )
    # project_id = "2"
    # component_id = "RobotXSpark12"
    # field_types =IOFieldType.objects.raw("select * from db_model_iofieldtype a where a.project_id='{project_id}' "
    #                         "and a.component_id in (select container_id from db_model_container b "
    #                         "where b.project_id='{project_id}' and b.component_id='{component_id}')".format(
    #     project_id = project_id,
    #     component_id = component_id
    # ))
    # for field_type in field_types:
    #     print(field_type.field, field_type.field_type,
    #             field_type.database_type, field_type.date_format,field_type.date_size, field_type.ignore)
    # from executor.Component1 import RobotXSpark
    # robotx1 = RobotXSpark('2','RobotXSpark112')
    # robotx1.load_from_db()
    # robotx1.prepare()
    # from executor.FeatureCombine import FeatureCombineComp
    # fc = FeatureCombineComp('4', 'FeatureCombine4')
    # fc.__load_from_db__()
    # fc.prepare()
    # from executor.components.AtomTest import AtomTest
    #
    # learn = AtomTest('3', 'AtomTest107')
    # learn.__load_from_db__()
    # learn.prepare()
    # from django.db import connections
    # from django.db.backends.mysql.base import DatabaseWrapper
    # from pymysql import connections as myconnection
    # conn = connections['default']
    # assert isinstance(conn, DatabaseWrapper)
    # print(len(connections.all()))
    # conn.ensure_connection()
    # conn.connection.close()
    # print(conn.is_usable())
    # from db_model.models import Task
    # print(len(Task.objects.filter()))
    from executor.celery_tasks import fake_task
    from db_engine import celery_app
    result = fake_task.delay()
    result = fake_task.delay()
    import time
    time.sleep(5)
    cancel_res = celery_app.control.revoke(result.id, terminate=True)


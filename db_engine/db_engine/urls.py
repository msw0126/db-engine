"""db_engine URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.views.generic import RedirectView

from db_engine import component, hive_reader, io, robotx_spark, xml, executor, self_defined_feature, feature_combine, \
    algorithms, atom_learn, atom_act, atom_test, yarn_resource, my_data, deploy

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^component/get_id$', component.get_id),  # check
    url(r'^component/delete$', component.delete),  # check
    url(r'^hive/list_table$', hive_reader.list_table),  # check
    url(r'^hive/structure$', hive_reader.structure),  # check
    url(r'^hive_reader/preview$', hive_reader.preview),  # check
    url(r'^io/save_field_type$', io.save_field_type),  # check
    url(r'^io/load_field_type$', io.load_field_type),  # check
    url(r'^component/save_hive_reader$', component.save_hive_reader),  # check
    url(r'^component/load_hive_reader$', component.load_hive_reader),  # check
    url(r'^robotx_spark/save_container$', robotx_spark.save_container),
    url(r'^robotx_spark/save_relation$', robotx_spark.save_relation),
    url(r'^robotx_spark/delete_relation$', robotx_spark.delete_relation),
    url(r'^robotx_spark/check_configuration$', robotx_spark.check_configuration),
    url(r'^robotx_spark/save_xml$', robotx_spark.save_xml),
    url(r'^robotx_spark/load_xml$', robotx_spark.load_xml),
    url(r'^robotx_spark/container_fields$', robotx_spark.container_fields),
    url(r'^robotx_spark/view_table$', robotx_spark.view_table),
    url(r'^robotx_spark/download_dict$', robotx_spark.download_dict),
    url(r'^xml/save$', xml.save),
    url(r'^xml/load$', xml.load),
    url(r'^executor/execute$', executor.execute),
    url(r'^executor/status$',executor.execution_status),
    url(r'^executor/current_exec$',executor.current_execution),
    url(r'^executor/get_log$',executor.get_log),
    url(r'^executor/kill_task$', executor.kill_task),
    url(r'^executor/stop_all$', executor.stop_all),
    url(r'^self_defined_feature/upload$', self_defined_feature.upload),
    url(r'^self_defined_feature/save_field_type$', self_defined_feature.save_field_type),
    url(r'^self_defined_feature/load_field_type$', self_defined_feature.load_field_type),
    url(r'^self_defined_feature/preview', self_defined_feature.perview),
    url(r'^feature_combine/save_relation$', feature_combine.save_relation),
    url(r'^feature_combine/load_relation$', feature_combine.load_relation),
    url(r'^feature_combine/robotx_spark_key_fields$', feature_combine.robotx_spark_key_fields),
    url(r'^feature_combine/continaer_fields$', feature_combine.container_fields),
    url(r'^feature_combine/view_table$', feature_combine.view_table),
    url(r'^favicon\.ico$', RedirectView.as_view(url='/static/images/favicon.ico')),
    url(r'^algorithm/list$', algorithms.list),
    url(r'^algorithm/list_params$', algorithms.list_params),
    url(r'^atom_learn/save_with_default$', atom_learn.save_with_default),
    url(r'^atom_learn/save$', atom_learn.save),
    url(r'^atom_learn/load$', atom_learn.load),
    url(r'^atom_learn/report$', atom_learn.report),
    url(r'^atom_learn/report_pdf$', atom_learn.report_pdf),
    url(r'^atom_act/save$', atom_act.save),
    url(r'^atom_act/download_prediction$',atom_act.download_prediction),
    url(r'^atom_act/report$',atom_act.report),
    url(r'^atom_act/report_pdf$',atom_act.report_pdf),
    url(r'^atom_test/report_pdf$',atom_test.report_pdf),
    url(r'^atom_test/save$', atom_test.save),
    url(r'^atom_test/report$', atom_test.report),
    url(r'^yarn_resource/previous$', yarn_resource.previous),
    url(r'^yarn_resource/save$', yarn_resource.save),
    url(r'^mydata/csv_upload$', my_data.csv_upload),
    url(r'^mydata/csv_into_hive$', my_data.csv_into_hive),
    url(r'^mydata/perview$', my_data.perview),
    url(r'^mydata/list_table$', my_data.list_table),
    url(r'^mydata/delete_table$', my_data.delete_table),
    url(r'^mydata/search_table$', my_data.search_table),
    url(r'^deploy/export$', deploy.export)
]

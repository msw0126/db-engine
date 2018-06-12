import json

from deepdiff import DeepDiff

import setting, cluster_setting
from common.UTIL import COMPONENTS, to_json, mk_working_directory, extract_component_type
from executor.components.AtomAct import AtomAct
from executor.components.AtomCommon import report_for_gains_lift, report_for_max_criteria, \
    report_for_synthetical_metric, report_for_confusion_matrix, report_for_threshold_metric, \
    report_for_score_group_threshold, report_for_topN_metric_List
from executor.components.FeatureCombine import FeatureCombine
from executor.components.RobotXSpark import RobotXSpark
from executor.components.Component import Component
from db_model.models import HiveReader, ModelKFoldsSummary, ModelMaxCriteria, ModelGainLiftSummary, \
    ModelConfusionMatrix, ModelThresholdsMetric, ModelTopnMetricList, ModelScoreGroupThreshold, ModelSyntheticMetrics
from db_model.models import AtomTest as AtomTestModel
from executor.celery_tasks import atom_test_execute


class Config:

    def __init__(self, hive_table, test_data_path, act_fold_path, test_fold_path):
        self.hive_table = hive_table
        self.test_data_path = test_data_path
        self.act_fold_path = act_fold_path
        self.test_fold_path = test_fold_path


class AtomTest(Component):

    COMPONENT_TYPE = COMPONENTS.ATOM_TEST

    TEST_METRICS = "test_metrics.json"
    TASK_RELY = atom_test_execute
    CONFIG_FILE_NAME = "test_config.json"

    def __init__(self, project_id, component_id):
        super().__init__(project_id, component_id)
        self.config = None # type: Config

    def __load_from_db__(self):
        project_id = self.project_id
        component_id = self.component_id

        atom_test_model = AtomTestModel.objects.filter(project_id=project_id,component_id=component_id)
        if len(atom_test_model) == 0:
            raise Exception("ATOM TEST NOT CONFIGURED")
        atom_test_model = atom_test_model[0]
        assert isinstance(atom_test_model, AtomTestModel)

        input_comp_id = atom_test_model.input_comp_id
        atom_act_id = atom_test_model.atom_act_id

        # 测试数据路径
        test_data_path = None
        input_comp_type = extract_component_type(input_comp_id)
        if input_comp_type == COMPONENTS.HIVE_READER:
            # hive reader
            hive_reader = HiveReader.objects.filter(project_id=project_id, component_id=input_comp_id)
            if len(hive_reader) == 0:
                raise Exception("ATOM LEARN INPUT HIVE READER NOT FOUND")
            hive_reader = hive_reader[0]
            assert isinstance(hive_reader, HiveReader)
            input_table = hive_reader.table_name
            test_data_path = "%s.%s" %(setting.HIVE_INPUT_DB, input_table)
        elif input_comp_type == COMPONENTS.ROBOTX_SPARK:
            test_data_path = RobotXSpark.output_table(project_id, input_comp_id)
        elif input_comp_type == COMPONENTS.FEATURE_COMBINE:
            test_data_path = FeatureCombine.output_table(project_id, input_comp_id)

        # 模型路径
        act_fold_path = AtomAct.act_fold_path(project_id, atom_act_id)
        # act输出路径
        test_fold_path = self.test_fold_path(project_id, component_id)
        # hive table export path
        test_data_export_path = self.test_data_export_path(project_id, component_id)

        self.config = Config(test_data_path, test_data_export_path, act_fold_path,test_fold_path)

    def __eq__(self, other):
        diff = DeepDiff(self.config, other.config)
        return len(diff) == 0

    def prepare(self):
        config_json = to_json(self.config, indent=4)
        config_path = AtomTest.get_config_path(self.project_id, self.component_id)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_json)

    @staticmethod
    def get_config_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomTest.CONFIG_FILE_NAME)

    @staticmethod
    def test_fold_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/TEST" %(project_id, component_id)

    @staticmethod
    def test_data_export_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/data.csv" % (project_id, component_id)

    @staticmethod
    def get_test_metrics_local_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomTest.TEST_METRICS)

    @staticmethod
    def get_test_metrics_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/TEST/%s" % (project_id, component_id,
                                                                               AtomTest.TEST_METRICS)

    @staticmethod
    def generate_report(project_id, component_id):
        model_metrics = AtomTest.get_test_metrics_local_path(project_id, component_id)
        with open(model_metrics, "r") as f:
            model_metrics = json.load(f)

        # clean
        for table in [ModelSyntheticMetrics,
                      ModelMaxCriteria,
                      ModelGainLiftSummary,
                      ModelConfusionMatrix,
                      ModelThresholdsMetric,
                      ModelTopnMetricList,
                      ModelScoreGroupThreshold]:
            table.objects.filter(project_id=project_id, component_id=component_id).delete()
        # synthetic summary
        report_for_synthetical_metric(model_metrics, project_id, component_id)

        # gains lift
        report_for_gains_lift(model_metrics, project_id, component_id)

        # max_criteria_metric
        report_for_max_criteria(model_metrics, project_id, component_id)

        # confusion matrix
        report_for_confusion_matrix(model_metrics, project_id, component_id)

        # threshold metric
        report_for_threshold_metric(model_metrics, project_id, component_id)

        # score_group_threshold_10_dict
        report_for_score_group_threshold(model_metrics, project_id, component_id)

        # topn_metrics_10_lst
        report_for_topN_metric_List(model_metrics, project_id, component_id)

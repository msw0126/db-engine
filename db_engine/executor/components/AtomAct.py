import json

from deepdiff import DeepDiff

import setting, cluster_setting
from common.UTIL import COMPONENTS, to_json, mk_working_directory, extract_component_type
from executor.components.AtomLearn import AtomLearn
from executor.components.FeatureCombine import FeatureCombine
from executor.components.RobotXSpark import RobotXSpark
from executor.components.Component import Component
from db_model.models import AtomAct as AtomActModel, ModelPredictionBIns
from db_model.models import HiveReader
from executor.celery_tasks import atom_act_execute


class Config:

    def __init__(self,hive_table, act_data_path, learn_fold_path, act_fold_path):
        self.hive_table = hive_table
        self.act_data_path = act_data_path
        self.act_fold_path = act_fold_path
        self.learn_fold_path = learn_fold_path


class AtomAct(Component):

    PREDICTION_BIN = "prediction_bins.json"
    PREDICTION_CSV = "prediction.csv"
    COMPONENT_TYPE = COMPONENTS.ATOM_ACT

    TASK_RELY = atom_act_execute
    CONFIG_FILE_NAME = "act_config.json"

    def __init__(self, project_id, component_id):
        super().__init__(project_id, component_id)
        self.config = None # type: Config

    def __load_from_db__(self):
        project_id = self.project_id
        component_id = self.component_id

        atom_act_model = AtomActModel.objects.filter(project_id=project_id,component_id=component_id)
        if len(atom_act_model) == 0:
            raise Exception("ATOM ACT NOT CONFIGURED")
        atom_act_model = atom_act_model[0]
        assert isinstance(atom_act_model, AtomActModel)

        input_comp_id = atom_act_model.input_comp_id
        atom_learn_id = atom_act_model.atom_learn_id

        # 测试数据路径
        act_data_path = None
        input_comp_type = extract_component_type(input_comp_id)
        if input_comp_type == COMPONENTS.HIVE_READER:
            # hive reader
            hive_reader = HiveReader.objects.filter(project_id=project_id, component_id=input_comp_id)
            if len(hive_reader) == 0:
                raise Exception("ATOM LEARN INPUT HIVE READER NOT FOUND")
            hive_reader = hive_reader[0]
            assert isinstance(hive_reader, HiveReader)
            input_table = hive_reader.table_name
            act_data_path = "%s.%s" %(setting.HIVE_INPUT_DB, input_table)
        elif input_comp_type == COMPONENTS.ROBOTX_SPARK:
            act_data_path = RobotXSpark.output_table(project_id, input_comp_id)
        elif input_comp_type == COMPONENTS.FEATURE_COMBINE:
            act_data_path = FeatureCombine.output_table(project_id, input_comp_id)

        # 模型路径
        learn_fold_path = AtomLearn.learn_fold_path(project_id, atom_learn_id)
        # act输出路径
        act_fold_path = self.act_fold_path(project_id, component_id)
        # hive table export path
        act_data_export = self.act_data_export_path(project_id, component_id)

        self.config = Config(act_data_path, act_data_export, learn_fold_path,act_fold_path)

    def __eq__(self, other):
        diff = DeepDiff(self.config, other.config)
        return len(diff) == 0

    def prepare(self):
        config_json = to_json(self.config, indent=4)
        config_path = AtomAct.get_config_path(self.project_id, self.component_id)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_json)

    @staticmethod
    def get_config_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomAct.CONFIG_FILE_NAME)

    @staticmethod
    def act_fold_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/ACT" %(project_id, component_id)

    @staticmethod
    def act_data_export_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/data.csv" % (project_id, component_id)

    @staticmethod
    def get_prediction_bin_local_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomAct.PREDICTION_BIN)

    @staticmethod
    def get_prediction_bin_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/ACT/%s" % (project_id, component_id,
                                                                                AtomAct.PREDICTION_BIN)

    @staticmethod
    def get_prediction_csv_local_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomAct.PREDICTION_CSV)

    @staticmethod
    def get_prediction_csv_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/ACT/%s" % (project_id, component_id,
                                                                              AtomAct.PREDICTION_CSV)
    @staticmethod
    def generate_report(project_id, component_id):
        prediction_bins = AtomAct.get_prediction_bin_local_path(project_id, component_id)
        with open(prediction_bins, "r") as f:
            prediction_bins = json.load(f)['score_bins']

        ModelPredictionBIns.objects.filter(project_id=project_id, component_id=component_id).delete()
        bulk_list = list()
        interval = [i/100.0 for i in range(1, 101)]
        for intv, v in zip(interval,prediction_bins):
            bulk_list.append(
                ModelPredictionBIns(
                    project_id = project_id,
                    component_id = component_id,
                    bin = "%.2f-%.2f" %(intv-0.01, intv),
                    value = v
                )
            )
        ModelPredictionBIns.objects.bulk_create(bulk_list)

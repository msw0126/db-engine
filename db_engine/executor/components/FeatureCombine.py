from deepdiff import DeepDiff

import cluster_setting
from executor.components.Component import Component
from executor.components.SelfDefinedFeature import SelfDefinedFeature

import setting
from common.UTIL import COMPONENTS, to_json, mk_working_directory
from db_model.models import FeatureCombine as FeatureCombineModel
from db_model.models import SelfDefinedFeatureType, FeatureCombineRelation
from executor.celery_tasks import feature_combine_execute
from executor.components.RobotXSpark import RobotXSpark


class Config:

    def __init__(self, rel_table, rel_dict, csv_file, gen_table, gen_dict, delimiter=","):
        self.rel_table = rel_table
        self.rel_dict = rel_dict
        self.csv_file = csv_file
        self.gen_table = gen_table
        self.gen_dict = gen_dict
        self.delimiter = delimiter
        self.field_type = set()
        self.relation = set()

    def add_field(self, name, tp):
        self.field_type.add(FieldType(name, tp))

    def add_relation(self, a, b):
        self.relation.add(Relation(a,b))


class FieldType:

    def __init__(self, name, tp):
        self.name = name
        self.tp = tp


class Relation:
    def __init__(self, a, b):
        self.a = a
        self.b = b


class FeatureCombine(Component):

    COMPONENT_TYPE = COMPONENTS.FEATURE_COMBINE

    TASK_RELY = feature_combine_execute
    CONFIG_FILE_NAME = "feature_combine.json"

    GEN_TABLE = setting.HIVE_OUTPUT_DB + ".combine_%s_%s"
    GEN_DICT = cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/dict.csv"

    def __init__(self, project_id, component_id):
        super().__init__(project_id, component_id)
        self.config = None

    def __load_from_db__(self):
        project_id = self.project_id
        component_id = self.component_id

        gen_table = self.output_table(project_id, component_id)
        gen_dict = self.output_dict(project_id, component_id)
        # get csv file
        feature_combines = FeatureCombineModel.objects.filter(project_id=project_id, component_id=component_id)
        feature_combine = feature_combines[0]
        connected_robotx = feature_combine.robotx_spark_id
        connected_self_feature = feature_combine.self_defined_feature_id

        # delimiter
        delimiter = ","
        # 目标关联表
        rel_table = RobotXSpark.output_table(project_id, connected_robotx)
        rel_dict = RobotXSpark.output_dict(project_id, connected_robotx)
        # csv_file
        csv_file = SelfDefinedFeature.CSV_NAME

        self.config = Config(rel_table,rel_dict, csv_file, gen_table, gen_dict, delimiter)

        # field_type
        field_types = SelfDefinedFeatureType.objects.filter(project_id=project_id, component_id=connected_self_feature)
        for field_ in field_types:
            assert isinstance(field_, SelfDefinedFeatureType)
            self.config.add_field(field_.field, field_.field_type)

        # relation
        relations = FeatureCombineRelation.objects.filter(project_id=project_id, component_id=component_id)
        for rel in relations:
            assert isinstance(rel, FeatureCombineRelation)
            self.config.add_relation(rel.robotx_field, rel.self_defined_field)

    def __eq__(self, other):
        diff = DeepDiff(self.config, other.config)
        return len(diff) == 0

    def prepare(self):
        config_json = to_json(self.config, indent=4)
        config_path = mk_working_directory(self.project_id, self.component_id, FeatureCombine.CONFIG_FILE_NAME)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_json)

    @staticmethod
    def get_config_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, FeatureCombine.CONFIG_FILE_NAME)

    @staticmethod
    def output_table(project_id, component_id):
        return FeatureCombine.GEN_TABLE %(project_id, component_id)

    @staticmethod
    def output_dict(project_id, component_id):
        return FeatureCombine.GEN_DICT %(project_id, component_id)
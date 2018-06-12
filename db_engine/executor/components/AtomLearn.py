import zipfile

import os
from deepdiff import DeepDiff

import setting, cluster_setting
from F_SETTING import CLUSTER_COMPONENT_DIR
from common.UTIL import COMPONENTS, to_json, mk_working_directory, extract_component_type
from executor.components.AtomCommon import report_for_synthetical_metric, report_for_gains_lift, \
    report_for_max_criteria, report_for_confusion_matrix, report_for_threshold_metric, report_for_score_group_threshold, \
    report_for_topN_metric_List
from executor.components.FeatureCombine import FeatureCombine
from executor.components.RobotXSpark import RobotXSpark
from executor.components.Component import Component
from db_model.models import AtomLearn as AtomLearnModel, IOFieldType, ModelTopnMetricList, ModelScoreGroupThreshold
from db_model.models import AtomLearnParam,HiveReader
from db_engine.atom_learn import ALGORITHM_PARAMS
from setting import COMMON_PARAMS
from executor.celery_tasks import atom_learn_execute
import json
from db_model.models import \
    ModelDescription, \
    ModelSummary, \
    ModelBestParams, \
    ModelCoefficient, \
    ModelVariableImportance, \
    ModelSyntheticMetrics, \
    ModelKFoldsSummary, \
    ModelMaxCriteria, \
    ModelGainLiftSummary, \
    ModelConfusionMatrix, \
    ModelThresholdsMetric


ALGORITHM_COMMON_PARAMS = set([param_['name'] for param_ in COMMON_PARAMS])


class Config:

    def __init__(self, hive_table, train_data_path, dict_path, id_name, target_name, algorithm, learn_fold_path):
        self.hive_table = hive_table
        self.train_data_path = train_data_path
        self.dict_path = dict_path
        self.id_name = id_name
        self.target_name = target_name
        self.hparams = dict()
        self.learn_fold_path = learn_fold_path
        self.algorithm = algorithm

    def add_common_param(self, param, value):
        self.__setattr__(param, value)

    def add_hparams(self, param, value):
        self.hparams[param] = value


param_transform_dict = dict(
    boolean = lambda x,d: x=='true' if 'stand_for' not in d else d['stand_for'][0 if x=='true' else 1],
    int = lambda x,d:int(x),
    double = lambda x,d:float(x)
)


def param_transform(description, value_str):
    p_type = description['type']
    p_multiple = description['multiple']
    values = value_str.split(",") if p_multiple else [value_str]
    trans_method = param_transform_dict[p_type]
    values = [trans_method(value, description) for value in values]
    return values if p_multiple else values[0]


class AtomLearn(Component):

    COMPONENT_TYPE = COMPONENTS.ATOM_LEARN

    TASK_RELY = atom_learn_execute
    CONFIG_FILE_NAME = "learn_config.json"
    HIVE_READER_DICT_NAME = "hive_reader_dict.csv"
    MODEL_PROPERTIES = "model_properties.json"
    MODEL_METRICS = "model_metrics.json"
    EXPORT_MODEL_CONFIG = "learn_configuration.json"
    EXPORT_MODEL_MOJO = "AtomLearn_export_model.zip"
    ROBOTX_DICT = "dict.csv"

    def __init__(self, project_id, component_id):
        super().__init__(project_id, component_id)
        self.config = None # type: Config

    def __load_from_db__(self):
        project_id = self.project_id
        component_id = self.component_id

        atom_learn_model = AtomLearnModel.objects.filter(project_id=project_id,component_id=component_id)
        if len(atom_learn_model) == 0:
            raise Exception("ATOM LEARN NOT CONFIGURED")
        atom_learn_model = atom_learn_model[0]
        assert isinstance(atom_learn_model, AtomLearnModel)

        input_comp_id = atom_learn_model.input_comp_id
        feature_id = atom_learn_model.feature_id
        feature_target = atom_learn_model.feature_target
        algorithm = atom_learn_model.algorithm

        train_data_path = None
        dict_path = None
        # 训练数据路径
        input_comp_type = extract_component_type(input_comp_id)
        if input_comp_type == COMPONENTS.HIVE_READER:
            # hive reader
            hive_reader = HiveReader.objects.filter(project_id=project_id, component_id=input_comp_id)
            if len(hive_reader) == 0:
                raise Exception("ATOM LEARN INPUT HIVE READER NOT FOUND")
            hive_reader = hive_reader[0]
            assert isinstance(hive_reader, HiveReader)
            input_table = hive_reader.table_name
            train_data_path = "%s.%s" %(setting.HIVE_INPUT_DB, input_table)
            # 生成数据字典
            io_field_types = IOFieldType.objects.filter(project_id=project_id, component_id=input_comp_id, ignore=False, selected=True)
            with open(AtomLearn.hive_reader_dict_path(project_id, component_id), 'w', encoding='utf-8') as f:
                lines = list()
                lines.append("variable,type\n")
                for io_f_type_ in io_field_types:
                    assert isinstance(io_f_type_, IOFieldType)
                    lines.append('"%s",%s\n' %(io_f_type_.field, io_f_type_.field_type))
                f.writelines(lines)
            dict_path = AtomLearn.HIVE_READER_DICT_NAME
        elif input_comp_type == COMPONENTS.ROBOTX_SPARK:
            train_data_path = RobotXSpark.output_table(project_id, input_comp_id)
            dict_path = RobotXSpark.output_dict(project_id, input_comp_id)
        elif input_comp_type == COMPONENTS.FEATURE_COMBINE:
            train_data_path = FeatureCombine.output_table(project_id, input_comp_id)
            dict_path = FeatureCombine.output_dict(project_id, input_comp_id)

        # learn输出路径
        learn_fold_path = self.learn_fold_path(project_id, component_id)
        train_data_export = self.train_data_path(project_id, component_id)

        self.config = Config(train_data_path,train_data_export,dict_path,feature_id, feature_target,algorithm,learn_fold_path)

        if algorithm not in ALGORITHM_PARAMS:
            raise Exception("ALGORITHM %s NOT SUPPORTED" %algorithm)
        algorithm_params = ALGORITHM_PARAMS[algorithm]
        atom_learn_param = AtomLearnParam.objects.filter(project_id=project_id,component_id=component_id)
        if len(algorithm_params)!=len(atom_learn_param):
            raise Exception("ALGORITHM %s LUCK OF PARAMETER" %algorithm)
        for param in atom_learn_param:
            assert isinstance(param, AtomLearnParam)
            param_name = param.param_name
            param_value = param.param_value
            # 转换为真实参数
            param_description = algorithm_params[param_name]
            true_value = param_transform(param_description, param_value)
            if param_name in ALGORITHM_COMMON_PARAMS:
                # 通用参数
                self.config.add_common_param(param_name, true_value)
            else:
                self.config.add_hparams(param_name, true_value)

    def __eq__(self, other):
        diff = DeepDiff(self.config, other.config)
        return len(diff) == 0

    def prepare(self):
        config_json = to_json(self.config, indent=4)
        config_path = AtomLearn.get_config_path(self.project_id, self.component_id)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_json)

    @staticmethod
    def get_config_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomLearn.CONFIG_FILE_NAME)

    @staticmethod
    def learn_fold_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/LEARN" %(project_id, component_id)

    @staticmethod
    def hive_reader_dict_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomLearn.HIVE_READER_DICT_NAME)

    @staticmethod
    def train_data_path(project_id, component_id):
        return cluster_setting.CLUSTER_WORKING_DIRECTORY + "/%s/%s/data.csv" % (project_id, component_id)

    @staticmethod
    def get_model_properties_local_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomLearn.MODEL_PROPERTIES)

    @staticmethod
    def get_model_properties_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/LEARN/%s" % (
            project_id, component_id, AtomLearn.MODEL_PROPERTIES)

    @staticmethod
    def get_model_metrics_local_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, AtomLearn.MODEL_METRICS)

    @staticmethod
    def get_model_metrics_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/LEARN/%s" % (
            project_id, component_id, AtomLearn.MODEL_METRICS)

    @staticmethod
    def get_robotx_dict_local_path(project_id, component_id):
        return mk_working_directory( project_id, component_id, AtomLearn.ROBOTX_DICT)

    @staticmethod
    def get_robotx_dict_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/%s" % (
            project_id, component_id, AtomLearn.ROBOTX_DICT)

    @staticmethod
    def get_export_model_config_local_path(project_id, component_id):
        return mk_working_directory( project_id, component_id, AtomLearn.EXPORT_MODEL_CONFIG)

    @staticmethod
    def get_export_model_config_hdfs_path(project_id, component_id):
        return cluster_setting.CLUSTER_DIRECTORY + "/%s/%s/LEARN/%s" % (
            project_id, component_id, AtomLearn.EXPORT_MODEL_CONFIG)

    @staticmethod
    def get_export_model_local_path(project_id, component_id):
        return mk_working_directory( project_id, component_id, AtomLearn.EXPORT_MODEL_MOJO )


    @staticmethod
    def get_export_model_hdfs_path(project_id, component_id):
        export_model_config_local_path = AtomLearn.get_export_model_config_local_path(project_id, component_id)
        with open( export_model_config_local_path, "r" ) as f:
            export_model_config = json.load( f )
            mojo_path = export_model_config["mojo_path"]
            mojo_hdfs_url = mojo_path.split( "/" )[3:]
            mojo_hdfs_path = "/" + "/".join( mojo_hdfs_url )
        return mojo_hdfs_path



    @staticmethod
    def get_zip_export_model_local_path(project_id, component_id, export_model_zipfile):
        return mk_working_directory(project_id, component_id, export_model_zipfile)

    @staticmethod
    def zip_export_model(project_id, component_id):
        #打包成zip包的名字
        export_model_zipfile = "%s_%s_export_model.zip" % ( project_id, component_id)
        #zip包存放的地址
        zip_export_model_local_path = AtomLearn.get_zip_export_model_local_path(project_id, component_id, export_model_zipfile)
        #从hdfs下载下来的model存放地址
        export_model_local_path = AtomLearn.get_export_model_local_path(project_id, component_id)
        #本地h2o-genmodel.jar文件的地址
        h2o_genmodel_jar_path = os.path.join(CLUSTER_COMPONENT_DIR, "AtomSpark", "h2o-genmodel.jar")
        f = zipfile.ZipFile( zip_export_model_local_path, 'w', zipfile.ZIP_DEFLATED )
        f.write(export_model_local_path, AtomLearn.EXPORT_MODEL_MOJO)
        f.write(h2o_genmodel_jar_path, "h2o-genmodel.jar")
        f.close()

    @staticmethod
    def generate_report(project_id, component_id):
        model_properties = AtomLearn.get_model_properties_local_path(project_id, component_id)
        model_metrics = AtomLearn.get_model_metrics_local_path(project_id, component_id)
        model_config = AtomLearn.get_config_path(project_id, component_id)
        with open(model_properties, "r") as f:
            model_properties = json.load(f)
        with open(model_metrics, "r") as f:
            model_metrics = json.load(f)
        with open(model_config, "r") as f:
            model_config = json.load(f)

        # clean
        for table in [ModelDescription,
                      ModelSummary,
                      ModelBestParams,
                      ModelCoefficient,
                      ModelVariableImportance,
                      ModelSyntheticMetrics,
                      ModelKFoldsSummary,
                      ModelMaxCriteria,
                      ModelGainLiftSummary,
                      ModelConfusionMatrix,
                      ModelThresholdsMetric,
                      ModelTopnMetricList,
                      ModelScoreGroupThreshold]:
            table.objects.filter(project_id=project_id, component_id=component_id).delete()

        # model description
        target = model_config["target_name"]
        algorithm = model_config["algorithm"]
        tuning_metric = "AUC"
        n_folds = None
        train_percent = None
        validation_percent = None
        actual_nfolds = model_properties["actual_params_dict"]["nfolds"]
        if actual_nfolds == 0:
            train_percent = 70
            validation_percent = 30
        else:
            n_folds = actual_nfolds

        ModelDescription(project_id=project_id,
                         component_id=component_id,
                         algorithm=algorithm,
                         target=target,
                         tuning_metric=tuning_metric,
                         n_folds=n_folds,
                         train_percent=train_percent,
                         validation_percent=validation_percent).save()

        # model summary
        summary_lst = model_properties["summary_lst"]
        header = summary_lst[0][1:]
        bulk_insert = list()
        for idx, values in enumerate(summary_lst[1:]):
            values = values[1:]
            for n_, v_ in zip(header, values):
                bulk_insert.append(
                    ModelSummary(project_id=project_id,
                                 component_id=component_id,
                                 layer=idx + 1,
                                 name=n_,
                                 value=str(v_)
                                 )
                )
        ModelSummary.objects.bulk_create(bulk_insert)

        # best params
        bulk_insert = list()
        actual_params = model_properties["actual_params_dict"]
        for n_ in model_config["hparams"].keys():
            v_ = actual_params[n_]
            bulk_insert.append(
                ModelBestParams(project_id=project_id,
                                component_id=component_id,
                                name=n_,
                                value=str(v_))
            )
        ModelBestParams.objects.bulk_create(bulk_insert)

        # coefficient
        if 'coef_lst' in model_properties:
            bulk_insert = list()
            coef_lst = model_properties['coef_lst']
            for coefs in coef_lst[1:]:
                name = coefs[0]
                coefficient = coefs[1]
                standardized_coefficient = coefs[2]
                bulk_insert.append(
                    ModelCoefficient(project_id=project_id,
                                     component_id=component_id,
                                     name=name,
                                     coefficient=str(coefficient),
                                     standardized_coefficient=str(standardized_coefficient))
                )
            ModelCoefficient.objects.bulk_create(bulk_insert)

        # variable importance
        if 'varimp_lst' in model_properties:
            bulk_insert = list()
            varimp_lst = model_properties['varimp_lst']
            for varimp in varimp_lst[1:]:
                variable = varimp[0]
                rel_imp = varimp[1]
                scal_imp = varimp[2]
                percent = varimp[3]
                bulk_insert.append(
                    ModelVariableImportance(project_id=project_id,
                                            component_id=component_id,
                                            variable=variable,
                                            relative_importance=str(rel_imp),
                                            scaled_importance=str(scal_imp),
                                            percentage=str(percent))
                )
            ModelVariableImportance.objects.bulk_create(bulk_insert)

        # synthetic summary
        report_for_synthetical_metric(model_metrics, project_id, component_id)

        # cross validation metrics
        if 'cv_summary_lst' in model_properties:
            bulk_insert = list()
            cv_summary = model_properties['cv_summary_lst']
            header = cv_summary[0][1:]
            for mtc in cv_summary[1:]:
                metric = mtc[0]
                for vtype, v_ in zip(header, mtc[1:]):
                    bulk_insert.append(
                        ModelKFoldsSummary(project_id=project_id,
                                           component_id=component_id,
                                           metric=metric,
                                           value_type=vtype,
                                           value=str(v_))
                    )
            ModelKFoldsSummary.objects.bulk_create(bulk_insert)

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
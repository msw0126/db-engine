from db_model.models import ModelDescription, ModelBestParams, ModelSummary, ModelCoefficient, ModelVariableImportance, \
    ModelSyntheticMetrics, ModelKFoldsSummary, ModelThresholdsMetric, ModelGainLiftSummary, ModelMaxCriteria, \
    ModelTopnMetricList, ModelConfusionMatrix, ModelScoreGroupThreshold, ModelPredictionBIns


def value_trim(v):
    try:
        v_float = float(v)
        v_int = int(v_float)
        if v_int == v_float:
            return str(v_int)
        if "." in v:
            v_ = v.split(".")
            v = v_[0] + "." + v_[1][0:4]
    except:
        pass
    return '-' if v == '' else v


def model_summary_grid(model_summary, algorithm):
    header = list()
    data = list()
    if algorithm == "DL":
        header.append('layer')
        for model_summary_ in model_summary:
            layer = int(model_summary_.layer)
            if len(data) < layer:
                for i in range(layer - len(data)):
                    data.append([str(layer)])
            if model_summary_.name == 'layer':
                continue
            if model_summary_.name not in header:
                header.append(model_summary_.name)
            v = model_summary_.value
            data[layer - 1].append(value_trim(v))
    else:
        data_ = list()
        for model_summary_ in model_summary:
            header.append(model_summary_.name)
            v = model_summary_.value
            data_.append(value_trim(v))
        data.append(data_)

    return header, data


def model_query(model, project_id, component_id, other_params=None):
    if other_params is None:
        return model.objects.filter(project_id=project_id, component_id=component_id)
    else:
        return model.objects.filter(project_id=project_id, component_id=component_id, **other_params)


def model_description(project_id, component_id):
    description = model_query(ModelDescription, project_id, component_id)
    description = description[0]
    algorithm = description.algorithm
    k_fold = description.n_folds

    return algorithm, k_fold


def model_best_param(project_id, component_id):
    best_params = model_query(ModelBestParams, project_id, component_id)
    best_params_name = list()
    best_params_value = list()
    for best_param_ in best_params:
        best_params_name.append(best_param_.name)
        best_params_value.append(value_trim(best_param_.value))
    return best_params_name, best_params_value


def model_summary(project_id, component_id, algorithm):
    summary = model_query(ModelSummary, project_id, component_id)
    return model_summary_grid(summary, algorithm)


def model_variable_importance(project_id, component_id, algorithm):
    if algorithm == 'LR':
        coefficient = model_query(ModelCoefficient, project_id, component_id)
        variable_title = "系数表"
        variable_head = ['variable', 'coefficient', 'standardized_coeffcients']
        variable_grid = []
        for coeff in coefficient:
            variable_grid.append([
                coeff.name,
                value_trim(coeff.coefficient),
                value_trim(coeff.standardized_coefficient),
            ])
    elif algorithm == 'NB':
        return None, None, None
    else:
        variable_imp = model_query(ModelVariableImportance, project_id, component_id)
        variable_title = "变量重要性"
        variable_head = ['variable', 'relative_importance', 'scaled_importance', 'percentage']
        variable_grid = []
        for imp in variable_imp:
            variable_grid.append([
                imp.variable,
                value_trim(imp.relative_importance),
                value_trim(imp.scaled_importance),
                value_trim(imp.percentage)
            ])

    return variable_title, variable_head, variable_grid


def model_synthetic_metrics(project_id, component_id):
    synthetic_metrics = model_query(ModelSyntheticMetrics, project_id, component_id)
    synthetic_metrics_grid_head = []
    synthetic_metrics_grid_data = []
    for metirc_ in synthetic_metrics:
        synthetic_metrics_grid_head.append(metirc_.name)
        synthetic_metrics_grid_data.append(value_trim(metirc_.value))
    synthetic_metrics_grid = [synthetic_metrics_grid_head, synthetic_metrics_grid_data]

    synthetic_metrics = {
        metirc_.name: float(metirc_.value)
        for metirc_ in synthetic_metrics
    }
    return synthetic_metrics, synthetic_metrics_grid


def model_k_fold_metric(project_id, component_id):
    k_fold_summary = model_query(ModelKFoldsSummary, project_id, component_id, other_params=dict(
        metric__in=["accuracy", "auc", "f0point5", "f1", "f2", "logloss", "mse", "precision", "r2", "recall", "rmse",
                    "specificity"]
    ))
    k_fold_head = ['']
    k_fold_data = []
    current_metric = None
    current_data = None
    head_lock = False
    for kfs in k_fold_summary:
        if current_metric != kfs.metric:
            if current_metric is not None:
                head_lock = True
            current_metric = kfs.metric
            current_data = [kfs.metric]
            k_fold_data.append(current_data)
        current_data.append(value_trim(kfs.value))
        if not head_lock:
            k_fold_head.append(kfs.value_type)

    return k_fold_head, k_fold_data


def threshold_tpr_fpr_recall_precision(project_id, component_id):
    fpr_cls = model_query(ModelThresholdsMetric, project_id, component_id, dict(metric='fpr')) \
        .extra({'t_threshold': "CAST(threshold AS decimal(3,2))"}) \
        .order_by('t_threshold')
    tpr_cls = model_query(ModelThresholdsMetric, project_id, component_id, dict(metric='tpr')) \
        .extra({'t_threshold': "CAST(threshold AS decimal(3,2))"}) \
        .order_by('t_threshold')
    recall_cls = model_query(ModelThresholdsMetric, project_id, component_id, dict(metric='recall')) \
        .extra({'t_threshold': "CAST(threshold AS decimal(3,2))"}) \
        .order_by('t_threshold')
    precision_cls = model_query(ModelThresholdsMetric, project_id, component_id, dict(metric='precision')) \
        .extra({'t_threshold': "CAST(threshold AS decimal(3,2))"}) \
        .order_by('t_threshold')

    threshold = list()
    fpr = list()
    tpr = list()
    recall = list()
    precision = list()
    for fpr_, tpr_, recall_, precision_ in zip(fpr_cls, tpr_cls, recall_cls, precision_cls):
        threshold.append(1 - float(fpr_.threshold))
        fpr.append(float(fpr_.value))
        tpr.append(float(tpr_.value))
        recall_ = float(recall_.value)
        if recall_ != 0:
            recall.append(recall_)
            precision.append(float(precision_.value))
    return threshold, fpr, tpr, recall, precision


def model_gain_lift(project_id, component_id):
    gain_lift = model_query(ModelGainLiftSummary, project_id, component_id)
    lft_fraction = [float(gl_.cumulative_data_fraction) for gl_ in gain_lift]
    lft = [float(gl_.node1cumulative_lift) for gl_ in gain_lift]
    cum_capture = [float(gl_.node1cumulative_capture_ratenode1) for gl_ in gain_lift]
    return lft_fraction, lft, cum_capture


def model_max_criteria_grid(project_id, component_id):
    max_criteria = model_query(ModelMaxCriteria, project_id, component_id)
    max_criteria_grid_head = ["metric", "threshold", "value"]
    max_criteria_grid_data = [
        [max_criteria_.metric, value_trim(max_criteria_.threshold), value_trim(max_criteria_.value)]
        for max_criteria_ in max_criteria
    ]
    return max_criteria_grid_head, max_criteria_grid_data


def model_top_metric_grid(project_id, component_id):
    topN_metric_head = ['score_TopN','tps','fps','tns','fns','recall','precision']
    topN_metric = model_query(ModelTopnMetricList, project_id, component_id)
    topN_metric_data = [
        [tm_.score_topN, value_trim(tm_.tps), value_trim(tm_.fps),
         value_trim(tm_.tns), value_trim(tm_.fns),
         value_trim(tm_.recall), value_trim(tm_.precision)]
        for tm_ in topN_metric]
    return topN_metric_head, topN_metric_data


def model_confusion_matrix(project_id, component_id, threshold):
    confusion_matrix_sql = "select * from " \
        "db_model_modelconfusionmatrix where  project_id='{0}' " \
        "and component_id='{1}' "\
        "and threshold = (select max(cast(threshold as decimal(3,2)))" \
        "from db_model_modelconfusionmatrix where project_id='{0}'" \
        "and component_id='{1}' " \
        "and cast(threshold as decimal(3,2)) <= {2})".format(project_id, component_id, threshold)
    confusion_matrix = ModelConfusionMatrix.objects.raw(confusion_matrix_sql)
    confusion_matrix_head = ['','0（预测值）','1（预测值）','Error','ErrorRate']
    confusion_matrix_data = [['0（真实值）'],['1（真实值）'],['Total']]
    for idx, cm_ in enumerate(confusion_matrix):
        idx = int(idx/4)
        if '/' in cm_.value:
            v_a, v_b = tuple(cm_.value.split('/'))
            v_a = int(float(v_a.replace("(", "").strip()))
            v_b = int(float(v_b.replace(")", "").strip()))
            confusion_matrix_data[idx].append("(%d / %d)" %(v_a,v_b))
        else:
            confusion_matrix_data[idx].append(value_trim(cm_.value))
    return confusion_matrix_head, confusion_matrix_data


def model_threshold_metric(project_id, component_id, threshold):
    threshold_metric_sql = "select * from " \
        "db_model_modelthresholdsmetric where  project_id='{0}' " \
        "and component_id='{1}' "\
        "AND metric in ('f1','f2','recall','precision','f0point5','specificity','accuracy') "\
        "and threshold = (select max(cast(threshold as decimal(3,2)))" \
        "from db_model_modelthresholdsmetric where project_id='{0}'" \
        "and component_id='{1}' " \
        "and cast(threshold as decimal(3,2)) <= {2})".format(project_id, component_id, threshold)
    threshold_metric = ModelConfusionMatrix.objects.raw(threshold_metric_sql)
    threshold_metric_label = [mc_.metric for mc_ in threshold_metric]
    threshold_metric_value = [float(mc_.value) for mc_ in threshold_metric]
    return threshold_metric_label, threshold_metric_value


def model_threshold_score(project_id, component_id, threshold):
    threshold_score_sql = "select * from " \
        "db_model_modelscoregroupthreshold where  project_id='{0}' " \
        "and component_id='{1}' "\
        "and threshold = (select max(cast(threshold as decimal(3,2)))" \
        "from db_model_modelscoregroupthreshold where project_id='{0}'" \
        "and component_id='{1}' " \
        "and cast(threshold as decimal(3,2)) <= {2})".format(project_id, component_id, threshold)
    threshold_score = ModelScoreGroupThreshold.objects.raw(threshold_score_sql)
    threshold_score_head = ['score_bins','tps','fps','tns','fns','recall','precision','accuracy','specificity']
    threshold_score_data = [
        [tsd.score_bins, int(float(tsd.tps)), int(float(tsd.fps)), int(float(tsd.tns)), int(float(tsd.fns)),
         value_trim(tsd.recall), value_trim(tsd.precision), value_trim(tsd.accuracy),
         value_trim(tsd.specificity)]
        for tsd in threshold_score
    ]
    return threshold_score_head, threshold_score_data


def prediction_bins(project_id, component_id, threshold):
    predictions = model_query(ModelPredictionBIns, project_id, component_id)
    total_num = 0
    zero_num = 0
    bin_num = [0]*10
    for pred in predictions:
        n = float(pred.value)
        total_num += n
        max_score = float(pred.bin.split("-")[0])
        if max_score <= threshold:
            zero_num += n
        bin_num[int(max_score*10)] += n
    bin_num = [n*1.0/total_num for n in bin_num]
    return total_num, zero_num, bin_num

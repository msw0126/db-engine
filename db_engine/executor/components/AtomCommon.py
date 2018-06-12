from db_model.models import ModelSyntheticMetrics, ModelGainLiftSummary, ModelMaxCriteria, ModelConfusionMatrix, \
    ModelThresholdsMetric, ModelTopnMetricList, ModelScoreGroupThreshold


def report_for_synthetical_metric(model_metrics, project_id, component_id):
    bulk_insert = list()
    synthetical_metric = model_metrics['synthetical_metric_dict']
    for n_, v_ in synthetical_metric.items():
        if n_ in ['AUC', 'Gini', 'r2', 'RMSE', 'logloss', 'MSE', 'AIC']:
            bulk_insert.append(
                ModelSyntheticMetrics(
                    project_id=project_id,
                    component_id=component_id,
                    name=n_,
                    value=str(v_)
                )
            )
    ModelSyntheticMetrics.objects.bulk_create(bulk_insert)


def report_for_gains_lift(model_metrics, project_id, component_id):
    bulk_insert = list()
    gains_lift_lst = model_metrics['gains_lift_lst']
    for v_ in gains_lift_lst[1:]:
        bulk_insert.append(
            ModelGainLiftSummary(
                project_id=project_id,
                component_id=component_id,
                group=v_[1],
                cumulative_data_fraction=str(v_[2]),
                node1lower_threshold=str(v_[3]),
                node1lift=str(v_[4]),
                node1cumulative_lift=str(v_[5]),
                node1response_rate=str(v_[6]),
                node1cumulative_response_rate=str(v_[7]),
                node1capture_rate=str(v_[8]),
                node1cumulative_capture_ratenode1=str(v_[9]),
                node1gain=str(v_[10]),
                node1cumulative_gain=str(v_[11])
            )
        )
    ModelGainLiftSummary.objects.bulk_create(bulk_insert)


def report_for_max_criteria(model_metrics, project_id, component_id):
    bulk_insert = list()
    max_criteria_metric_lst = model_metrics['max_criteria_metric_lst']
    for v_ in max_criteria_metric_lst[1:]:
        bulk_insert.append(
            ModelMaxCriteria(
                project_id=project_id,
                component_id=component_id,
                metric=str(v_[0]),
                threshold=str(v_[1]),
                value=str(v_[2]),
                idx=str(v_[3])
            )
        )
    ModelMaxCriteria.objects.bulk_create(bulk_insert)


def report_for_confusion_matrix(model_metrics, project_id, component_id):
    # c_matrix_lst
    bulk_insert = list()
    c_matrix_lst = model_metrics['c_matrix_lst']
    for v_ in c_matrix_lst:
        threshold = int(v_['threshold'] * 100) / 100.0
        value = v_['value']
        head = value[0][1:]
        for vv_ in value[1:]:
            vtype = vv_[0]
            for param_v, hd in zip(vv_[1:], head):
                bulk_insert.append(
                    ModelConfusionMatrix(
                        project_id=project_id,
                        component_id=component_id,
                        threshold=str(threshold),
                        value_type="%s-%s" % (hd, vtype),
                        value=str(param_v)
                    )
                )
    ModelConfusionMatrix.objects.bulk_create(bulk_insert)


def report_for_threshold_metric(model_metrics, project_id, component_id):
    # thresholds_scores_df_s_lst
    bulk_insert = list()
    thresholds_scores = model_metrics['thresholds_scores_df_s_lst']
    head = thresholds_scores[0]

    threshold_idx = [idx for idx,k in enumerate(head) if k=='threshold'][0]

    for v_ in thresholds_scores[1:]:
        threshold = v_[threshold_idx]
        for param_v, param_n in zip(v_, head):
            if param_n == 'threshold':
                continue
            bulk_insert.append(
                ModelThresholdsMetric(
                    project_id=project_id,
                    component_id=component_id,
                    threshold=str(threshold),
                    metric=param_n,
                    value=str(param_v)
                )
            )
    ModelThresholdsMetric.objects.bulk_create(bulk_insert)


def report_for_topN_metric_List(model_metrics, project_id, component_id):
    bulk_insert = list()
    topn_metrics_10_lst = model_metrics['topn_metrics_10_lst']
    for topn_metrics_i in topn_metrics_10_lst[1:]:
        bulk_insert.append(
            ModelTopnMetricList(
                project_id=project_id,
                component_id=component_id,
                score_topN=topn_metrics_i[0],
                tps=topn_metrics_i[1],
                fps=topn_metrics_i[2],
                tns=topn_metrics_i[3],
                fns=topn_metrics_i[4],
                recall=topn_metrics_i[5],
                precision=topn_metrics_i[6],
                accuracy=topn_metrics_i[7],
                specificity=topn_metrics_i[8]
            )
        )
    ModelTopnMetricList.objects.bulk_create(bulk_insert)


def report_for_score_group_threshold(model_metrics, project_id, component_id):
    bulk_insert = list()
    score_group_threshold_10_dict = model_metrics['score_group_threshold_10_dict']
    score_group_threshold_10_dict = sorted(score_group_threshold_10_dict, key=lambda x: float(x['threshold']))
    for v_ in score_group_threshold_10_dict:
        threshold = int(float(v_['threshold']) * 100) / 100.0
        value = v_['value']
        for group_v in value[1:]:
            bulk_insert.append(
                ModelScoreGroupThreshold(
                    project_id=project_id,
                    component_id=component_id,
                    threshold=str(threshold),
                    score_bins=str(group_v[0]),
                    tps=str(group_v[1]),
                    fps=str(group_v[2]),
                    tns=str(group_v[3]),
                    fns=str(group_v[4]),
                    recall=str(group_v[5]),
                    precision=str(group_v[6]),
                    accuracy=str(group_v[7]),
                    specificity=str(group_v[8])
                )
            )
    ModelScoreGroupThreshold.objects.bulk_create(bulk_insert)
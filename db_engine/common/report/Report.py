from reportlab.platypus import Spacer, PageBreak

from common.report.Tool import NameTitle, first_title, normal_text, second_title, param_list, grid, roc, \
    precision_recall, lift, ks, gain, hist, hist_score_bin
from common.report import Parts


def learn_report(project_id, component_id, doc_width, threshold_confusion, threshold_top):

    algorithm, k_fold = Parts.model_description(project_id, component_id)
    best_params_name, best_params_value = Parts.model_best_param(project_id, component_id)
    summary_header, summary_data = Parts.model_summary(project_id, component_id, algorithm)
    variable_title, variable_head, variable_grid = Parts.model_variable_importance(project_id, component_id, algorithm)

    story = []
    story += [NameTitle("ATOM-Learn执行报告", doc_width), Spacer(6, 50)]
    story += first_title("算法参数设置")
    story += normal_text("算法名称：%s" % algorithm)

    story += second_title("最优参数组")
    story += param_list([best_params_name, best_params_value], doc_width)

    story += second_title("默认参数值")
    model_summary = [summary_header] + summary_data
    story += param_list(model_summary, doc_width)

    if variable_title is not None:
        story += second_title(variable_title)
        story += grid(variable_head, variable_grid, doc_width)

    story += [PageBreak()]
    story += learn_act_common(project_id, component_id, doc_width, threshold_confusion, threshold_top, k_fold)

    return story


def test_report(project_id, component_id, doc_width, threshold_confusion, threshold_top):
    story = []

    story += [NameTitle("ATOM-Test执行报告", doc_width), Spacer(6, 50)]
    story += learn_act_common(project_id, component_id, doc_width, threshold_confusion, threshold_top)

    return story


def act_report(project_id, component_id, doc_width, threshold):
    story = []
    story += [NameTitle("ATOM-Act执行报告", doc_width), Spacer(6, 50)]
    total_num, zero_num, bin_num = Parts.prediction_bins(project_id, component_id, threshold)
    story += first_title("样本概况")
    story += normal_text("样本数：%d" % total_num)
    story += first_title("打分分布")
    story += hist_score_bin(doc_width, bin_num)
    story += first_title("正负样本量（阈值=%.2f）" % threshold)
    story += hist(doc_width, ['0', '1'], [zero_num, total_num-zero_num])
    return story


def learn_act_common(project_id, component_id, doc_width, threshold_confusion, threshold_top, k_fold=None):
    synthetic_metrics, synthetic_metrics_grid = Parts.model_synthetic_metrics(project_id, component_id)
    threshold, fpr, tpr, recall, precision = Parts.threshold_tpr_fpr_recall_precision(project_id, component_id)
    lft_fraction, lft, cum_capture = Parts.model_gain_lift(project_id, component_id)
    max_criteria_grid_head, max_criteria_grid_data = Parts.model_max_criteria_grid(project_id, component_id)
    confusion_matrix_head, confusion_matrix_data = Parts.model_confusion_matrix(project_id, component_id, threshold_confusion)
    threshold_metric_label, threshold_metric_value = Parts.model_threshold_metric(project_id, component_id, threshold_confusion)
    threshold_score_head, threshold_score_data = Parts.model_threshold_score(project_id, component_id, threshold_top)
    topN_metric_head, topN_metric_data = Parts.model_top_metric_grid(project_id, component_id)

    story = []
    story += first_title("模型综合评估指标")

    if k_fold is not None:
        k_fold_head, k_fold_data = Parts.model_k_fold_metric(project_id, component_id)
        story += second_title('交叉验证模型概要')
        story += grid(k_fold_head, k_fold_data, doc_width)

    story += second_title("综合评估指标")
    story += param_list(synthetic_metrics_grid, doc_width, font_size=9, col_with_percent=1)

    story += second_title("ROC")
    story += roc(doc_width, synthetic_metrics['AUC'], fpr, tpr)

    story += second_title("Precision/Recall")
    story += precision_recall(doc_width, recall, precision)

    story += second_title("Lift")
    story += lift(doc_width, lft_fraction, lft)

    story += second_title("KS")
    story += ks(doc_width, threshold, tpr, fpr)

    story += second_title("Gain")
    story += gain(doc_width, lft_fraction, cum_capture)

    story += [PageBreak()]
    story += first_title("模型评估指标")
    story += second_title("最大标准量得分阀值表")
    story += grid(max_criteria_grid_head, max_criteria_grid_data, doc_width)

    story += second_title("测量集混淆矩阵和评估指标（阈值=%.2f）" % threshold_confusion)
    story += grid(confusion_matrix_head, confusion_matrix_data, doc_width, first_col_strong=True)

    story += hist(doc_width, threshold_metric_label, threshold_metric_value)

    story += second_title("按score分组统计（阈值=%.2f）" % threshold_top)
    story += grid(threshold_score_head, threshold_score_data, doc_width, ajust=True)

    story += second_title("Top维度指标统计")
    story += grid(topN_metric_head, topN_metric_data, doc_width)

    return story

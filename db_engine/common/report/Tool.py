import io
from math import ceil

import os

from F_SETTING import FONT_DIR

import matplotlib
matplotlib.use('Agg')
from matplotlib.font_manager import FontProperties
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase.pdfmetrics import registerFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Flowable, Table, TableStyle
from reportlab.platypus.flowables import HRFlowable, Spacer, Image
from reportlab.platypus.para import Paragraph
from matplotlib import pyplot as plt


plot_hei = FontProperties(fname=os.path.join(FONT_DIR, 'simhei.ttf'))
matplotlib.rcParams['axes.unicode_minus'] = False

song = TTFont('song', os.path.join(FONT_DIR, 'simsun.ttc'))
hei = TTFont('hei', os.path.join(FONT_DIR, 'simhei.ttf'))
registerFont(song)
registerFont(hei)

normalStyle = getSampleStyleSheet()['Normal']
normalStyle.fontName = "song"


class BoxTitle(Flowable):
    def __init__(self, width, height, text, font, font_size, color):
        Flowable.__init__(self)
        self.width = width
        self.height = height
        self.text = text
        self.font = font
        self.color = color
        self.font_size = font_size

    def __repr__(self):
        return "BoxTitle(w=%s, h=%s, t=%s)" % (self.width, self.height, self.text)

    def draw(self):
        self.canv.saveState()
        self.canv.setFillColor(self.color)
        self.canv.setStrokeColor(self.color)
        self.canv.rect(0, 0, self.width, self.height, stroke=1, fill=1)
        self.canv.restoreState()
        self.canv.setFont(self.font, self.font_size)
        self.canv.drawString(3*self.width, 0, self.text)


class NameTitle(Flowable):

    def __init__(self, text, width, font='hei', font_size=20):
        Flowable.__init__(self)
        self.text = text
        self.font = font
        self.width = width
        self.font_size = font_size

    def draw(self):
        self.canv.saveState()
        self.canv.setFont(self.font, self.font_size)
        self.canv.drawCentredString(0.5*self.width, 0, self.text)
        self.canv.restoreState()


def first_title(text):
    return [BoxTitle(3, 10, text, 'hei', 12, '#2992F1'),
            HRFlowable(width="100%", color="#2992F1", spaceBefore=10, spaceAfter=5)]


def second_title(text):
    return [Paragraph('<para autoLeading="off" align="left"><font face="hei" fontsize="10">%s</font></para>' % text, normalStyle),
            Spacer(6, 10)]


def normal_text(text):
    return [Paragraph('<para autoLeading="off" align="left"><font face="song" fontsize="9">%s</font></para>' % text, normalStyle),
            Spacer(6, 20)]


def param_list(param, width ,font_size=6, col_with_percent=0.618):
    title = param[0]
    value = param[1:]

    col_width = []
    current_len = 0
    for vs in zip(title, *tuple(value)):
        len_col = max([len(v) for v in vs]+[5])*font_size*col_with_percent
        current_len += len_col
        if current_len > width:
            break
        col_width.append(len_col)

    if len(col_width)<len(title):
        title = title[0:len(col_width)]
        value = [v[0:len(col_width)] for v in value]

    title_table = Table([title], colWidths=col_width, hAlign="LEFT")
    title_table.setStyle(TableStyle([
        ('FONTSIZE', (0, 0), (-1, -1), font_size),
        ('FACE', (0, 0), (-1, -1), 'hei'),
        ('TEXTCOLOR', (0, 0), (-1, 0), '#2992f1')
    ]))

    value_table = Table(value, colWidths=col_width, hAlign="LEFT")
    value_table.setStyle(TableStyle([
        ('FONTSIZE', (0, 0), (-1, -1), font_size),
        ('FACE', (0, 0), (-1, -1), 'song')
    ]))

    return [HRFlowable(width="100%", color="#d4dbe2", spaceBefore=1, spaceAfter=4),
            title_table,
            HRFlowable(width="100%", color="#d4dbe2", spaceBefore=2, spaceAfter=4),
            value_table,
            HRFlowable(width="100%", color="#d4dbe2", spaceBefore=2, spaceAfter=2),
            Spacer(6, 20)]


def grid(tt, data, width, first_col_strong=False, ajust=False):
    if ajust:
        tvs = [max([len(i),5]) for i in tt]
        slen = width*1.0/sum(tvs)
        col_width = [slen*i for i in tvs]
    else:
        col_width = [width/len(tt)]*(len(tt))

    num_per_line = int(col_width[0] / 4) - 2
    for dt in data:
        lines = ceil(len(dt[0]) / num_per_line)
        dt[0] = "\n".join([dt[0][j * num_per_line: (j + 1) * num_per_line] for j in range(lines)])

    title_table = Table([tt]+data, colWidths=col_width, hAlign="LEFT")
    title_table.setStyle(TableStyle([
        ('TEXTCOLOR', (0, 0), (-1, 0), 'white'),
        ('BACKGROUND', (0, 0), (-1, 0), '#2992F1'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), ['#eff5fe', 'white']),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('FACE', (0, 0), (-1, -1), 'song'),
        ('FACE', (0, 0), (0, -1), 'hei' if first_col_strong else 'song')
    ]))
    return [title_table, Spacer(6, 20)]


def ylabel(*args,**kwargs):
    kwargs['fontsize'] = 21
    kwargs['fontproperties'] = plot_hei
    plt.ylabel(*args, **kwargs)


def xlabel(*args, **kwargs):
    kwargs['fontsize'] = 21
    kwargs['fontproperties'] = plot_hei
    plt.xlabel(*args, **kwargs)


def title(*args, **kwargs):
    kwargs['fontsize'] = 23
    kwargs['fontproperties'] = plot_hei
    plt.title(*args, **kwargs)


def __plot_common_(width, method, p=0.48):
    fig = plt.figure(figsize=(15, 15*p))
    plt.grid(axis="y", linestyle="-", color="#637a8f", linewidth=2)

    method()

    # plt.xticks(fontsize=12)
    # plt.yticks(fontsize=12)

    ax = plt.gca()
    ax.set_axisbelow(True)
    ax.spines['top'].set_color("none")
    ax.spines['right'].set_color("none")
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png')
    img_data.seek(0)
    im = Image(img_data, width=width, height=width * p)
    return [im, Spacer(6, 20)]


def hist(width, label, data):

    def __hist__():
        bar_size = 0.50
        idx = range(1, len(label) + 1)
        plt.bar(idx, data, color='#ffb65f', width=bar_size, edgecolor='white')
        # plt.xticks([x + bar_size / 2 for x in idx], label)
        # plt.axis([idx[0] + bar_size - 1, idx[-1] + 1, 0, int(ceil(max(data) * 10)) / 10])
        plt.xticks(idx, label)
        plt.axis([idx[0] - 1 + bar_size/2, idx[-1] + 1 - bar_size/2, 0, int(ceil(max(data) * 10)) / 10])

    return __plot_common_(width, __hist__, p=0.37)


def hist_score_bin(width, data):

    def __hist_score_bin__():
        bar_size = 0.1
        idx = [i/10+0.05 for i in range(0, 10)]
        plt.bar(idx, data, color='#ff9bae', width=bar_size, edgecolor='white')
        plt.axis([0, 1, 0, int(ceil(max(data) * 100)) / 100])
        xlabel("分类区间")
        ylabel("占比")

    return __plot_common_(width, __hist_score_bin__)


def roc(width, auc_score, fpr, tpr):

    def __roc__():
        title(u"测试数据Area Under the Curve=%.2f" % auc_score)
        plt.plot(fpr, tpr, c='#ff4266', linewidth=3)
        plt.fill_between(fpr, tpr, 0, color='#ff617f', alpha='0.2')
        plt.axis([0, 1, 0, 1])
        ylabel(u"tpr")
        xlabel(u"fpr")
        plt.plot([0, 1], [0, 1], c="#ff9e20", linewidth=2)

    return __plot_common_(width, __roc__)


def precision_recall(width, recall, precision):

    def __precision_recall__():
        plt.plot(recall, precision, c='#26dabe', linewidth=3)
        plt.axis([0, 1, 0, 1])
        ylabel(u"precision")
        xlabel(u"recall")

    return __plot_common_(width, __precision_recall__)


def lift(width, fraction, lft):

    def __lift__():
        plt.plot(fraction, lft, c='#26dabe', linewidth=3)
        plt.axis([0, 1, 0, int(ceil(max(lft) * 10)) / 10])

        ylabel(u"lift")
        xlabel(u"fraction")

    return __plot_common_(width, __lift__)


def gain(width, fraction, cum_capture):

    def __gain__():
        plt.plot(fraction, cum_capture, c='#26dabe', linewidth=3)
        plt.axis([0, 1, 0, int(ceil(max(cum_capture) * 10)) / 10])
        ylabel(u"true positive rate")
        xlabel(u"fraction")

    return __plot_common_(width, __gain__)


def ks(width, scores, tpr, fpr):
    threshold_max = -1
    tpr_max = -1
    fpr_max = -1
    ks_max = -1
    for score_, tpr_, fpr_ in zip(scores, tpr, fpr):
        gap = tpr_-fpr_
        if gap > ks_max:
            threshold_max = score_
            tpr_max = tpr_
            fpr_max = fpr_
            ks_max = gap

    def __ks__():
        plt.plot(scores, tpr, c='#ff4266', linewidth=3)
        plt.plot(scores, fpr, c='#2184ff', linewidth=3)
        plt.plot([threshold_max, threshold_max], [fpr_max, tpr_max], linestyle="--", linewidth=2, c='#ff9e2c')
        plt.axis([0, 1, 0, 1])
        ylabel(u"累计占比")
        xlabel(u"按分组打分")
        title(u"测试数据KS=%d%%" % int(100 * ks_max), loc='left')
        legend = plt.legend(['正样本洛伦兹曲线', '负样本洛伦兹曲线', 'KS值'], prop=plot_hei,
                            bbox_to_anchor=(0.3, 0.97, 0.7, 0.1), ncol=3, mode="expand")
        legend.get_frame().set_linewidth(0.0)
        legend.get_title().set_fontsize(fontsize=22)

    return __plot_common_(width, __ks__)


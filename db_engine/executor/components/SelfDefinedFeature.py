from common.UTIL import mk_working_directory


class SelfDefinedFeature(object):

    CSV_NAME = "data.csv"

    @staticmethod
    def csv_file_path(project_id, component_id):
        return mk_working_directory(project_id, component_id, SelfDefinedFeature.CSV_NAME)

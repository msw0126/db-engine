import copy
from common.UTIL import extract_component_type

class Line:
    def __init__(self, start, end):
        self.start = start
        self.end = end


class Point:
    def __init__(self, id=None):
        self.relies = set()
        self.forwards = set()
        self.id = id
        self.relies_bak = set() # type: set[Point]
        if id is not None:
            self.type = extract_component_type(id)

    def add_rely(self, rely):
        self.relies.add(rely)

    def remove_rely(self, rely, bag: set):
        if rely in self.relies:
            self.relies.remove(rely)
        if len(self.relies) == 0:
            bag.add(self)

    def shoot(self, bag):
        for point in self.forwards:
            assert isinstance(point, Point)
            point.remove_rely(self, bag)

    def add_forward(self, forward):
        self.forwards.add(forward)

    def __repr__(self):
        return str(self.id)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.id == other
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

class Toplogy:
    def __init__(self):
        self.points = dict()  # type: dict[str, Point]
        self.levels = None # type: list[set[Point]]

    def add_line(self, start, end):
        if start not in self.points:
            point = Point(start)
            self.points[start] = point
        if end not in self.points:
            point = Point(end)
            self.points[end] = point

        end_point = self.points[end]
        start_point = self.points[start]
        start_point.add_forward(end_point)
        end_point.add_rely(start_point)

    def sort(self):
        for point in self.points.values():
            point.relies_bak = copy.copy(point.relies)
        self.levels = list()
        remove = set()
        remove.add(Point())
        while len(self.points)>0:
            current_level = set()
            for point in self.points.values():
                for  removed_point in remove:
                    point.remove_rely(removed_point, current_level)
            for point in current_level:
                del self.points[point.id]
            remove = current_level
            self.levels.append(current_level)

    def get_previous_component(self, point_id):
        levels = copy.copy(self.levels)
        level_of_point = None
        for idx,level in enumerate(levels):
            if level_of_point is None:
                if point_id in level:
                    level_of_point = idx
                    break
                else:
                    continue
        levels = levels[:level_of_point+1]
        top_point = None
        for p in levels[level_of_point]:
            if p.id == point_id:
                top_point = p
                break
        levels[level_of_point] = [top_point]
        idx = level_of_point-1
        high_level = set()
        while idx >= 0:
            new_level = set()
            high_level |= set(levels[idx+1])
            for point in levels[idx]:
                if len(point.forwards&high_level)>0:
                    new_level.add(point)
            levels[idx] = list(new_level)
            idx -= 1
        high_level |= set(levels[idx+1])
        # 移除要执行节点
        high_level.remove(levels[level_of_point][0])
        return levels, high_level



if __name__ == "__main__":
    toplogy = Toplogy()
    toplogy.add_line("01", "21")
    toplogy.add_line("01", "12")
    toplogy.add_line("01", "22")
    toplogy.add_line("01", "11")
    toplogy.add_line("21", "32")
    toplogy.add_line("12", "21")
    toplogy.add_line("12", "22")
    toplogy.add_line("11", "31")
    toplogy.add_line("22", "32")
    toplogy.add_line("22", "33")
    toplogy.add_line("22", "31")
    toplogy.add_line("32", "41")
    toplogy.add_line("33", "42")
    toplogy.add_line("31", "42")
    toplogy.add_line("41", "51")
    toplogy.add_line("42", "51")

    toplogy.sort()
    toplogy.get_previous_component('31')
from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_one():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id: int):
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    :param discipline_id:
    :return:
    """
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r


def select_three(discipline_):
    """
    Знайти середній бал у групах з певного предмета.
    :return:
    """
    result = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Group)\
        .where(Discipline.id == discipline_)\
        .group_by(Discipline.name, Group.name)\
        .order_by(desc("avg_grade"))\
        .all()

    return result


def select_four():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    :return:
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label("avg_score"))\
        .select_from(Grade)\
        .one()

    return result


def select_five(teacher):
    """
    Знайти які курси читає певний викладач.
    :return:
    """
    result = session.query(Teacher.fullname, Discipline.name)\
        .select_from(Discipline)\
        .join(Teacher)\
        .where(Teacher.id == teacher)\
        .all()

    return result


def select_six(group_id):
    """
    Знайти список студентів у певній групі.
    :return:
    """
    result = session.query(Group.name, Student.fullname)\
        .select_from(Student)\
        .join(Group)\
        .where(Group.id == group_id)\
        .all()

    return result


def select_seven(group_id, discipline_id):
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    :return:
    """
    result = session.query(Student.fullname, Group.name, Discipline.name, Grade.grade)\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Group)\
        .where(and_(Group.id == group_id, Discipline.id == discipline_id)) \
        .group_by(Student.fullname, Group.name, Discipline.name, Grade.grade) \
        .order_by(desc(Grade.grade))\
        .all()

    return result


def select_eight(teacher_id):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return:
    """
    result = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .where(Teacher.id == teacher_id)\
        .group_by(Teacher.fullname)\
        .order_by(desc("avg_grade"))\
        .one()

    return result


def select_nine(student_id):
    """
    Знайти список курсів, які відвідує певний студент.
    :return:
    """
    result = session.query(Student.fullname, Discipline.name)\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .where(Student.id == student_id)\
        .group_by(Discipline.name, Student.fullname)\
        .order_by(Discipline.name)\
        .all()

    return result


def select_ten(student_id, teacher_id):
    """
    Список курсів, які певному студенту читає певний викладач.
    :return:
    """
    result = session.query(Student.fullname, Teacher.fullname, Discipline.name)\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Teacher)\
        .where(and_(Student.id == student_id, Teacher.id == teacher_id))\
        .group_by(Student.fullname, Teacher.fullname, Discipline.name)\
        .order_by(Discipline.name)\
        .all()

    return result


def select_additional_1(teacher_id, student_id):
    """
    Середній бал, який певний викладач ставить певному студентові.
    :return:
    """
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Teacher)\
        .where(and_(Teacher.id == teacher_id, Student.id == student_id))\
        .group_by(Teacher.fullname, Student.fullname)\
        .order_by(desc("avg_grade"))\
        .all()

    return result


def select_additional_2(discipline_id, group_id):
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    :param discipline_id:
    :param group_id:
    :return:
    """
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


if __name__ == '__main__':
    # print(select_one())
    # print(select_two(1))
    # print(select_three(1))
    # print(select_four())
    # print(select_five(2))
    # print(select_six(2))
    # print(select_seven(1, 2))
    # print(select_eight(2))
    # print(select_nine(1))
    # print(select_ten(2, 2))
    print(select_additional_1(2, 2))
    # print(select_additional_2(1, 2))

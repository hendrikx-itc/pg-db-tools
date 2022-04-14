#! /usr/bin/python3
"""
Call: python3 orderedmerge.py <file1> <file2>
Result: All elements in the form they are in file1, but in the order of file2.
  All elements from file1 that are not in file2 are put at the end.
"""
import sys

from pg_db_tools.pg_types import load, PgDatabase, PgObject
from pg_db_tools.commands.extract_from_db import format_yaml


def extended_type(object: PgObject):
    if object.object_type == "function":
        return tuple([arg.data_type.to_json(short=True) for arg in object.arguments])
    else:
        return object.object_type


def main(source, ordersource):
    result = PgDatabase()
    objectdict = dict(
        ((obj.schema.name, obj.name, extended_type(obj)), obj) for obj in source.objects
    )
    for object in ordersource.objects:
        if (object.schema.name, object.name, extended_type(object)) in objectdict:
            result.objects.append(
                objectdict[(object.schema.name, object.name, extended_type(object))]
            )
    for object in objectdict.values():
        if object not in result.objects:
            result.objects.append(object)
    return format_yaml(result.to_json(internal_order=True))


if __name__ == "__main__":
    source = load(open(sys.argv[1]).read())
    ordersource = load(open(sys.argv[2]).read())
    main(source, ordersource)

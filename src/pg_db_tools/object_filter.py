"""
Provides all database object filtering classes
"""


class ObjectFilter:
    def include(self, db_object):
        """
        Return True if the object should be included and false otherwise
        """
        raise NotImplementedError()


class TableExclusionFilter(ObjectFilter):
    def __init__(self, exclusion_list):
        self.exclusion_list = exclusion_list

    def include(self, pgtable):
        return pgtable.name not in self.exclusion_list


class TableInclusionFilter(ObjectFilter):
    def __init__(self, inclusion_list):
        self.inclusion_list = inclusion_list

    def include(self, pgtable):
        return pgtable.name in self.inclusion_list


class DatabaseFilter:
    """
    Provides collection of all filtering settings and methods to apply those
    settings.
    """
    def __init__(self, table_filters, type_filters):
        self.table_filters = table_filters
        self.type_filters = type_filters

    def include_table(self, pgtable):
        """
        Return True if the table should be included and False otherwise
        """
        return all(
            table_filter.include(pgtable)
            for table_filter in self.table_filters
        )

    def include_type(self, pgtype):
        """
        Return True if the type should be included and False otherwise
        """
        return all(
            type_filter.include(pgtype)
            for type_filter in self.type_filters
        )

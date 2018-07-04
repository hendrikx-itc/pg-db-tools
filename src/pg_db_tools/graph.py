import networkx


def database_to_graph(database):
    graph = networkx.DiGraph()

    for schema in database.schemas.values():
        for table in schema.tables:
            graph.add_node(table)

    return graph

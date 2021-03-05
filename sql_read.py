def sql_read(file, delimiter = ";"):
    """Reads multiline SQL text document and splits text by delimiter.
       Returns list of queries as collapsed strings."""
    with open(file) as file_object:
        queries = file_object.read().split(delimiter)
    for i in range(len(queries)):
        queries[i] = queries[i].strip()
    while '' in queries:
        queries.remove('')
    processed_queries = []
    for query in queries:
        query_split = query.split("\n")
        processed_queries.append(" ".join(query_split) + delimiter)
    return processed_queries

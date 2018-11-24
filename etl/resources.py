
'''
Renames columns resulting from a groupby operation
back to their original column names.
EG. 'sum(value)' -> 'value'
EG. 'avg(value)' -> 'value'
'''
def renameGroupedColumns(cols):
    return [c[c.find("(")+1:c.find(")")] if '(' in c else c for c in cols]

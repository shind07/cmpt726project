import csv

'''
This file contains any variables/functions/etc that are used in multiple files.
'''

def renameGroupedColumns(cols):
    '''
    Renames columns resulting from a groupby/agg operation
    back to their original column names.
    Eg. 'sum(value)' -> 'value'
    Eg. 'avg(value)' -> 'value'
    '''
    return [c[c.find("(")+1:c.find(")")] if '(' in c else c for c in cols]

# Final columns to be included in parsed box score data
box_score_columns = ['Gender', 'Year', 'Division','Date', 'Time', 'Team', 'FGM', 'FGA', \
        '3FG', '3FGA', 'FT', 'FTA', 'PTS', 'ORebs', 'DRebs', 'Tot Reb', 'AST', 'TO', 'STL', \
        'BLK', 'Fouls',  'opp_Team', 'opp_FGM', 'opp_FGA', 'opp_3FG', 'opp_3FGA', 'opp_FT', 'opp_FTA',\
        'opp_PTS', 'opp_ORebs', 'opp_DRebs', 'opp_Tot Reb', 'opp_AST', 'opp_TO', 'opp_STL', 'opp_BLK', 'opp_Fouls', ]

def matchStrings(str1, str2, degree=1):
    min_length = min(len(str1), len(str2))
    degree = min_length if degree > min_length else degree
    #print("Degree: ", degree)
    set1, set2 = set(), set()

    def _setify(str, size):
        return {str[i:i+size] for i in range(len(str) - size + 1)}

    for i in range(degree):
        set1 = set1 | _setify(str1, i+1)
        set2 = set2 | _setify(str2, i+1)
    #print(set1, set2)
    max_length = max(len(set1), len(set2))
    return len(set1 & set2) / float(max_length)

def csvToArray(path):
    with open(path) as f:
        csv_reader = csv.reader(f)
        return [row for row in csv_reader]

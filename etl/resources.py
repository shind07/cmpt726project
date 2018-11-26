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

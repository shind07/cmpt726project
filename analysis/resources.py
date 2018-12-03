'''
This file contains any variables/functions/etc that are used in multiple files.
'''
import csv
from pyspark.sql import functions, types



def renameGroupedColumns(cols):
    '''
    Renames columns resulting from a groupby/agg operation
    back to their original column names.
    Eg. 'sum(value)' -> 'value'
    Eg. 'avg(value)' -> 'value'
    '''
    return [c[c.find("(")+1:c.find(")")] if '(' in c else c for c in cols]

play_by_play_schema_raw = types.StructType([
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Period', types.StringType(), False),
    types.StructField('TimeLeft', types.StringType(), False),
    types.StructField('Score', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Status', types.StringType(), False),
    types.StructField('Action', types.StringType(), False),
    types.StructField('Shot_Clock', types.IntegerType(), False),
    types.StructField('Lineup', types.StringType(), False),
    types.StructField('Lineup_Time', types.StringType(), False),
])

play_by_play_schema_parsed = types.StructType([
    types.StructField('Gender', types.StringType(), False),
    types.StructField('Year', types.StringType(), False),
    types.StructField('Division', types.StringType(), False),
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Score', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Status', types.StringType(), False),
    types.StructField('Action', types.StringType(), False),
    types.StructField('Shot_Clock', types.IntegerType(), False),
    types.StructField('Lineup', types.StringType(), False),
    types.StructField('Seconds_Left', types.IntegerType(), False),
    types.StructField('Away_Score', types.IntegerType(), False),
    types.StructField('Home_Score', types.IntegerType(), False),
    types.StructField('Home_Margin', types.IntegerType(), False),
    types.StructField('File_Team', types.StringType(), False),
])

box_score_schema_raw = types.StructType([
    types.StructField('Date', types.StringType(), False),
    types.StructField('Time', types.StringType(), False),
    types.StructField('Period', types.StringType(), False),
    types.StructField('Team', types.StringType(), False),
    types.StructField('Player', types.StringType(), False),
    types.StructField('Pos', types.StringType(), False),
    types.StructField('MP', types.StringType(), False),
    types.StructField('FGM', types.IntegerType()),
    types.StructField('FGA', types.IntegerType()),
    types.StructField('3FG', types.IntegerType()),
    types.StructField('3FGA', types.IntegerType()),
    types.StructField('FT', types.IntegerType()),
    types.StructField('FTA', types.IntegerType()),
    types.StructField('PTS', types.IntegerType()),
    types.StructField('ORebs', types.IntegerType()),
    types.StructField('DRebs', types.IntegerType()),
    types.StructField('Tot_Reb', types.IntegerType()),
    types.StructField('AST', types.IntegerType()),
    types.StructField('TO', types.IntegerType()),
    types.StructField('STL', types.IntegerType()),
    types.StructField('BLK', types.IntegerType()),
    types.StructField('Fouls', types.IntegerType()),
])

# Final columns to be included in parsed box score data
box_score_columns = ['Gender', 'Year', 'Division','Date', 'Time', 'File_Team', 'Team', 'FGM', 'FGA', \
        '3FG', '3FGA', 'FT', 'FTA', 'PTS', 'ORebs', 'DRebs', 'Tot_Reb', 'AST', 'TO', 'STL', \
        'BLK', 'Fouls',  'opp_Team', 'opp_FGM', 'opp_FGA', 'opp_3FG', 'opp_3FGA', 'opp_FT', 'opp_FTA',\
        'opp_PTS', 'opp_ORebs', 'opp_DRebs', 'opp_Tot_Reb', 'opp_AST', 'opp_TO', 'opp_STL', 'opp_BLK', 'opp_Fouls', ]
box_score_columns_strings = ['Gender', 'Year', 'Division','Date', 'Time', 'File_Team', 'Team', 'opp_Team']
box_score_schema_parsed = types.StructType(
    [types.StructField(field_name, types.StringType(), True) if field_name in box_score_columns_strings else types.StructField(field_name, types.IntegerType(), True) \
    for field_name in box_score_columns]
)

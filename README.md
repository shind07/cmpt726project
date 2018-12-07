# cmpt726project
CMPT 726 Final Project

## Executing the Data Pipeline
### 1) Preliminary Data Reading/Cleaning/Parsing
#### 1a) Parse Box Scores:
`spark-submit etl/parse_box_score.py <input> <output>`  

Example: `cmpt732project> spark-submit etl/parse_box_score.py sample_data output/box-score`

#### 1b) Parse Play-By-Plays:
`spark-submit etl/parse_play_by_play.py <input> <output>`  

Example: `cmpt732project> spark-submit etl/parse_play_by_play.py sample_data output/play-by-play`

#### 1c) Get Home Teams:
`spark-submit etl/parse_home_team.py <input> <output>`

Example:  `cmpt732project> spark-submit etl/parse_home_team.py sample_data output/home-teams`

#### 1c) Create Dataset for Machine Learning:


Example:  `cmpt732project> spark-submit etl/parse_home_team.py sample_data output/home-teams`

### 2) More ETL and Data analysis
#### 2a) Calculate Box Score Advanced Stats:
`spark-submit analysis/parse_box_score.py <box-score-data> <output>`  
Example: `spark-submit analysis/calc_box_stats.py output/box output/box-stats`

#### 2b) Create Dataset for Machine Learning:
`spark-submit etl/create_ml_data.py <play-by-play-data> <box-score-data> <home-team-data> <output>`
Example: `spark-submit etl/create_ml_data.py output/pbp output/box output/box-stats-teams output/home-teams output/ml`


Create data for Machine Learning Project:
spark-submit etl/create_ml_data.py output/pbp-test output/box-test output/box-stats-test-teams output/home-teams-test output/ml

Calculate Points Per Shot for each action:
spark-submit analysis/pps_by_action.py ./output/pbp-test output/pps-action

Calculate Points Per Shot by time on shot clock:
spark-submit analysis/pps_by_shot_clock.py ./output/pbp-test output/pps-shot-clock

Calculate number of assists per action:
spark-submit analysis/assists_by_action.py ./output/pbp-test output/assists-action

Calculate number of assists to and from specific players:
spark-submit analysis/assists_by_player.py ./output/pbp-test output/assists-player

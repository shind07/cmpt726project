# CMPT 732 Final Project
### Eugene Borissov and Scott Hind

## Executing the Data Pipeline
### 1) Preliminary Data Reading/Cleaning/Parsing
#### 1a) Parse Box Scores:
`spark-submit etl/parse_box_score.py <input> <output>`  

Example: `cmpt732project> spark-submit etl/parse_box_score.py data/sample_data output/box-score`

#### 1b) Parse Play-By-Plays:
`spark-submit etl/parse_play_by_play.py <input> <output>`  

Example: `cmpt732project> spark-submit etl/parse_play_by_play.py data/sample_data output/play-by-play`

#### 1c) Get Home Teams:
`spark-submit etl/parse_home_team.py <input> <output>`

Example:  `cmpt732project> spark-submit etl/parse_home_team.py data/sample_data output/home-teams`


### 2) More ETL and Data analysis
#### 2a) Calculate Box Score Advanced Stats:
`spark-submit analysis/parse_box_score.py <box-score-data> <output>`  

Example: `cmpt732project> spark-submit analysis/calc_box_stats.py output/box output/box-stats`

#### 2b) Create Dataset for Machine Learning:
`spark-submit etl/create_ml_data.py <play-by-play-data> <box-score-data> <home-team-data> <output>`

Example: `cmpt732project> spark-submit etl/create_ml_data.py output/pbp output/box output/box-stats-teams output/home-teams output/ml`

#### 2c) Calculate Points Per Shot For Each Action:
`spark-submit analysis/pps_by_action.py <play-by-play-data> <output>`

Example: `cmpt732project>  spark-submit analysis/pps_by_action.py output/play-by-play output/pps-by-action`

#### 2c) Calculate Points Per Shot By Time Left on Shot Clock:
`spark-submit analysis/pps_by_shot_clock.py <play-by-play-data> <output>`

Example: `cmpt732project>  spark-submit analysis/pps_by_shot_clock.py output/play-by-play output/pps-by-shot-clock`

#### 2d) Calculate Number of Assists by Action
`spark-submit analysis/assists_by_action.py <play-by-play-data> <output>`

Example: `cmpt732project>  spark-submit analysis/assists_by_action.py output/play-by-play output/assists-by-action`

#### 2e) Calculate Number of Assists to and from Specific Players
`spark-submit analysis/assists_by_player.py <play-by-play-data> <output>`

Example: `cmpt732project>  spark-submit analysis/assists_by_player.py output/play-by-play output/assists-by-player`



## Simulating Streaming
### 1) Start the simulator:
`cmpt732project> python streaming/simulator.py`

### 2) Start Spark Streaming
`cmpt732project> spark-submit streaming/consumer.py`

## Running the Data Visualization Dashboard (Web Frontend)

The dashboard is split up into 3 repositories: The [frontend](https://github.com/shind07/frontend.git) (HMTL/JavaScript), [web server](https://github.com/shind07/backend.git) (Python Flask), and [backend](https://github.com/shind07/flask-server.git) (Python).

### 1) Download the 3 repositories and place them in the same directory

`cmpt732project> mkdir dashboard`  
`cmpt732project> cd dashboard`  
`dashboard> git clone https://github.com/shind07/frontend.git`  
`dashboard> git clone https://github.com/shind07/backend.git`  
`dashboard> git clone https://github.com/shind07/flask-server.git`  


### 2) Install the requirements

### 3) Run the backend and frontend

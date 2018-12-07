# cmpt726project
CMPT 726 Final Project

# Execution:
## 1) Preliminary data reading/cleaning/parsing
1a)Parse Box Scores:
spark-submit etl/parse_box_score.py <input> <output>
Example: spark-submit etl/parse_box_score.py sample_data output/box-score

1b)Parse Play-By-Plays:
spark-submit etl/parse_play_by_play.py <input> <output>
spark-submit etl/parse_play_by_play.py sample_data output/play-by-play

Get Home Teams:
spark-submit etl/parse_home_team.py <input> <output>
spark-submit etl/parse_home_team.py sample_data output/home-teams

## 2) Data analysis
Calculate Box Score Stats:
spark-submit analysis/calc_box_stats.py output/box-test output/box-stats-test
spark-submit analysis/calc_box_stats.py <box-directory> <output>

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

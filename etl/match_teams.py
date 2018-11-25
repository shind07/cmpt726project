from resources import csvToArray, match
import sys

def main(teams_path, match_path):

    teams = csvToArray(teams_path)
    matches = csvToArray(match_path)

    for t in teams:
        team = t[0]
        print(team)


if __name__ == '__main__':
    teams = sys.argv[1]
    matches = sys.argv[2]
    main(teams, matches)

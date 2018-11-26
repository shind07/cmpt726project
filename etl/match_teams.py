from resources import csvToArray, matchStrings
import sys

def parseSchoolNames(school):
    words_to_remove = ['University of', 'College of', 'College', 'University']
    for w in words_to_remove:
        school = school.replace(w, '')
    school = school.replace(' and ', ' & ')
    return school.replace('  ', ' ').strip()
def main(teams_path, match_path):

    teams = csvToArray(teams_path)
    matches = csvToArray(match_path)

    for t in teams:
        team = t[0]
        max_score = 0
        matched_team = ''
        for m in matches:
            match = parseSchoolNames(m[0])
            score = matchStrings(team, match, 3)
            if score > max_score:
                max_score = score
                matched_team = match
        print(team, matched_team, max_score)



if __name__ == '__main__':
    teams = sys.argv[1]
    matches = sys.argv[2]
    main(teams, matches)

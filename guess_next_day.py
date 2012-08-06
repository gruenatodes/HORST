import json
import urllib2
import postguess

# set up: standard result is 2:1 and 'guesses' is a dictionary mapping match_ids into a result
standard_result = '2:1'
guesses = {}

# load next matchday
url_whichday = 'http://openligadb-json.heroku.com/api/current_group?league_shortcut=bl1'
json_whichday = urllib2.urlopen(url_whichday).read()
whichday = json.loads(json_whichday)

group_id = whichday['group_order_id']

# request matches info for next matchday
url_matches = "http://openligadb-json.heroku.com/api/matchdata_by_group_league_saison?group_order_id=%s&league_saison=2012&league_shortcut=bl1" % group_id
matches = json.loads( urllib2.urlopen(url_matches).read() )
matches = matches['matchdata']

for game in matches:
    guesses[game['match_id']] = standard_result

# 'botliga_post' is a function taking a dictionary in the form of 'guesses' and posting it to botliga.de.
# The token is specified in the module 'postguess'.
print postguess.botliga_post(guesses)


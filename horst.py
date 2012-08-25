import sqlite3
from datetime import datetime
import json
import urllib2
import iso8601
from collections import defaultdict
import statsmodels.api as sm
import numpy as np
import postguess

db_name = 'data/example.db'
avg_days = [1, 5, 17, 34]
reg_names = []
for avg_len in avg_days:
    for team in ['home', 'guest']:
        for sc in ['scored', 'conceded']:
            reg_name = team + sc + str(avg_len)
            reg_names.append(reg_name)

conn = sqlite3.connect(db_name)
conn.row_factory = sqlite3.Row
c = conn.cursor()

def find_prev_and_next_day():
    c.execute('''SELECT season, day, date FROM bundesliga
                WHERE datetime(date)>datetime('now', '+2hours')
                ORDER BY date DESC LIMIT 1''')
    row = c.fetchone()
    next_day = row['day']
    season = row['season']

    c.execute('''SELECT season, day, date FROM bundesliga
                WHERE datetime(date)<datetime('now', '+2hours')
                AND NOT (season>=? AND day>=?)
                ORDER BY date DESC LIMIT 1''', (season, next_day))
    row = c.fetchone()
    prev_day = row['day']
    prev_season = row['season']


    return prev_day, prev_season, next_day, season

def enter_results_for_day(day, season):
    url = "http://openligadb-json.heroku.com/api/matchdata_by_group_league_saison?league_saison=%s&league_shortcut=bl1&group_order_id=%s" % (season-1, day)
    daydata = urllib2.urlopen(url).read()
    daydata = json.loads(daydata)['matchdata']
    # good thing we put home and guest id in the table! :)
    for game in daydata:
        home_id = game['id_team1']
        guest_id = game['id_team2']
        goals_home = game['points_team1']
        goals_guest = game['points_team2']
        c.execute('''UPDATE bundesliga SET scorehome=?, scoreguest=?
                     WHERE season=? AND day=? AND home_id=? AND guest_id=?''',
                     (goals_home, goals_guest, season, day, home_id, guest_id))
    conn.commit()
    print "Ergebnisse von Spieltag %s in Saison %s aktualisiert." %(day,season)

def update_matchtimes_for_day(season, day):
    c.execute('''SELECT hometeam, guestteam, date, home_id, guest_id
                 FROM bundesliga
                 WHERE season=? AND day=?''', (season, day))
    db_data = c.fetchall()
    url = "http://openligadb-json.heroku.com/api/matchdata_by_group_league_saison?league_saison=%s&league_shortcut=bl1&group_order_id=%s" % (season-1, day)
    daydata = urllib2.urlopen(url).read()
    daydata = json.loads(daydata)['matchdata']
    for db_game in db_data:
        for game in daydata:
            db_home_id = int(db_game['home_id'])
            db_guest_id = int(db_game['guest_id'])
            if (db_home_id == int(game['id_team1']) and
                  db_guest_id == int(game['id_team2'])):
                db_date = db_game['date']
                datetm = game['match_date_time']
                changes = []
                if iso8601.parse_date(db_date) == iso8601.parse_date(datetm):
                    pass
                else:
                    newdate = str(iso8601.parse_date(datetm))
                    newdate = newdate[:newdate.find('+')]
                    c.execute('''UPDATE bundesliga
                     SET date=?
                     WHERE season=? AND day=? AND home_id=? AND guest_id=?''',
                              (newdate, season, day, db_home_id, db_guest_id))
                    changes.append((db_game['hometeam'], db_game['guestteam']))
                break
    conn.commit()
    print "Anstosszeiten fuer Spieltag %s, %s \
wurden geaendert:" %(day,season), changes


def get_avg_goals(tname, season, day, length=1, sc='scored'):
    c.execute('''SELECT day FROM bundesliga
                         WHERE season=? GROUP BY day
                         ORDER by day DESC LIMIT 1''', (season-1,))
    last_day_prev = c.fetchone()[0]
    if sc == 'scored':
        goalnames = ('scorehome', 'scoreguest')
    if sc == 'conceded':
        goalnames = ('scoreguest', 'scorehome')
        
    query = "SELECT %s as 'teamscore' FROM bundesliga " % goalnames[0]
    query += "WHERE hometeam='%s' " %tname
    query += "AND ((season=%s AND day<%s AND day>=%s) " % (season, day,
                                                           day-length)
    query += "OR (season=%s AND day>=%s)) " % (season-1, last_day_prev-
                                               length+day)
    query += "UNION ALL "
    query += "SELECT %s as 'teamscore' FROM bundesliga " % goalnames[1]
    query += "WHERE guestteam='%s' " %tname
    query += "AND ((season=%s AND day<%s AND day>=%s) " % (season, day,
                                                           day-length)
    query += "OR (season=%s AND day>=%s))" % (season-1, last_day_prev-
                                               length+day)
    goallist = c.execute(query).fetchall()

    try:
        su = sum([game[0] for game in goallist]) / (len(goallist) + 0.0)
    except ZeroDivisionError:
        su = None
    
    return su
    

def update_regressors_for_day(season, day):
    # get info on day:
    rows = c.execute('''SELECT hometeam, guestteam FROM bundesliga
                 WHERE season=%s AND day=%s''' % (season, day))
    rows = rows.fetchall()
    # First do new/onew
    res = c.execute('''SELECT hometeam FROM bundesliga
                 WHERE season=%s GROUP BY hometeam''' % (season-1,))
    teams_prev_season = [game['hometeam'] for game in res]
    if len(teams_prev_season) > 0:
        for row in rows:
            reg_new, reg_onew = (0,0)
            if row['hometeam'] not in teams_prev_season:
                reg_new = 1
            if row['guestteam'] not in teams_prev_season:
                reg_onew = 1
            c.execute('''UPDATE bundesliga
                     SET new =?, onew=?
                     WHERE season=? AND day=? AND hometeam=?''',
                     (reg_new, reg_onew, season, day, row['hometeam']))
    # Then do goals for/against
    for row in rows:
        query = "UPDATE bundesliga SET "
        for avg_len in avg_days:
            for team in ['home', 'guest']:
                tname = row[team + 'team']
                for sc in ['scored', 'conceded']:
                    reg_name = team + sc + str(avg_len)
                    avg_goals = get_avg_goals(tname, season, day, avg_len, sc)
                    if avg_goals == None:
                        avg_goals = 'NULL'
                    else:
                        avg_goals = round(avg_goals, 2)
                    query += (reg_name + '=' + str(avg_goals) + ", ")
        query = query[:-2] + " " # remove last comma
        query += ("WHERE season=%s AND day=%s AND hometeam='%s'"
                  % (season, day, row['hometeam']))
        c.execute(query)
    conn.commit()

def get_data(this_season, next_day):
    c.execute('''SELECT season FROM bundesliga GROUP BY season
                 ORDER BY season ASC LIMIT 1''')
    first_season = int(c.fetchone()[0])
    
    query = '''SELECT * FROM bundesliga
           WHERE season>=? AND NOT (season >=? AND day >=?)
           ORDER BY season, day, date, hometeam'''

    data = []
    c.execute(query, (first_season+1, this_season, next_day))
    rows = c.fetchall()
    keys = rows[0].keys()
    for game in rows:
        dic = {}
        for k in keys:
            dic[k] = game[k]
            if dic[k] == None:
                dic[k] = -1
        data.append(dic)

    return data

def encode_results(results):
    unique_results = list(set(results))
    histo = defaultdict(int)
    for x in results:
        histo[x] += 1
##    for x in sorted(histo, key=histo.get, reverse=True):
##        print x, histo[x]

    outcome_codes = {} # this maps results into integer codes
    for result in unique_results:
        if result[0] == result[1]: # tie: (0,0): 0, (1,1): 1, (2,2) or higher: 2
            if result[0] <= 1:
                outcome_codes[result] = result[0] # 0,1 - 0:0, 1:1
            else:
                outcome_codes[result] = 2 # 2 - 2:2+
        elif result[0] > result[1]: # win: sort by (goal diff, goals scored)
            if result[0] == result[1] + 1:
                outcome_codes[result] = 2 + min(result[0], 2) # 3,4 - 1:0, 2:1+
            elif result[0] == result[1] + 2:
                outcome_codes[result] = 3 + min(result[0], 3) # 5,6 - 2:0, 3:1+
            elif result[0] == result[1] + 3: 
                outcome_codes[result] = 7 # 7 - 3:0+
            else: # result[0] >= result[1] + 4
                outcome_codes[result] = 12 # 13 - 4:0+
        else: # loss
            if result[0] == result[1] - 1:
                outcome_codes[result] = 7 + min(result[1], 2) # 8,9 - 0:1, 1:2+
            elif result[0] == result[1] - 2:
                outcome_codes[result] = 10 # 10 - 0:2+
            else: # result[0] >= result[1] - 3
                outcome_codes[result] = 11 # 11 - 0:3+

    outcomes = map(lambda x: outcome_codes[x], results)
    return outcomes, outcome_codes

def give_predictions(y, X, X_predict, decode, rows):
    print 'Now regressing...'
    estimators = sm.MNLogit(y, X).fit()
##    print estimators.summary()
    y_predict = np.round(estimators.predict(X_predict), 4)
    # find max element in prediction
    tips = []
    for i in range(len(y_predict)):
        dic = {}
        dic['home_id'] = rows[i]['home_id']
        dic['teams'] = (rows[i]['hometeam'], rows[i]['guestteam'])
        dic['pred_array'] = y_predict[i]
        dic['pred'] = decode[list(y_predict[i]).index(max(y_predict[i]))]
        print rows[i]['hometeam'], rows[i]['guestteam'], dic['pred']
        tips.append(dic)
    return tips

def regress_and_predict(season, day):
    data = get_data(season, day)
    outcomes, code_dict = encode_results(
                [(game['scorehome'],game['scoreguest']) for game in data])

    decode = {}
    for k, v in code_dict.iteritems():
        decode[v] = decode.get(v, [])
        decode[v].append(k)
    if None in decode: del decode[None]
    for k in decode:
        decode[k] = max(decode[k], key=lambda x: -x[0]-x[1])
        
    histo = defaultdict(int)
    for x in outcomes:
        histo[x] += 1
    print "Ein bisschen Geschichte:"
    for x in sorted(histo, key=histo.get, reverse=True):
        print x, decode[x], histo[x]

    # Construct regressors
    X = [] # list of lists
    y = [] # just a vector
    i = 0
    for game in data:
        regs = [1] # a constant
        for team, new in [['home', 'new'], ['guest', 'onew']]:
            for avg_len in avg_days:
                for sc in ['scored', 'conceded']:
                    reg_name = team + sc + str(avg_len)
                    regs.append((1-game[new]) * game[reg_name])
        regs.append(game['new'])
        regs.append(game['onew'])
        X.append(regs)
        y.append(outcomes[i])
        i += 1
    X = np.array(X)
    y = np.array(y)
    # predictors:
    query = ('''SELECT * FROM bundesliga
                WHERE season=%s AND day=%s
                ORDER BY date ASC, hometeam ASC'''
             % (season, day) )
    c.execute(query)
    rows = c.fetchall()
    X_predict = []
    for game in rows:
        regs = [1] # a constant
        for team, new in [['home', 'new'], ['guest', 'onew']]:
            for avg_len in avg_days:
                for sc in ['scored', 'conceded']:
                    reg_name = team + sc + str(avg_len)
                    if game[reg_name] != None:
                        reg = game[reg_name]
                    else:
                        reg = -1
                    regs.append((1-game[new]) * reg)
        regs.append(game['new'])
        regs.append(game['onew'])
        X_predict.append(regs)
    X_predict = np.array(X_predict)
    tips = give_predictions(y, X, X_predict, decode, rows)
    return tips, decode

def maximize_expected_points(tips, decode):
    point_dist = [5,3,2]
    lis = sorted(decode.keys())

    print "Nach Beruecksichtigung der erwarteten Punkte:"
    for j in range(len(tips)):
        game = tips[j]
        probs = {}
        for i in range(len(lis)):
            probs[lis[i]] = round(game['pred_array'][i],4)
        exp_pts = {}
        for outcome in sorted(decode.keys()):
            # find results that bring highest points (equality)
            exp_pts[outcome] = point_dist[0] * probs[outcome]
            # finc results that bring 2nd highest points (same goal diff)
            prob = 0
            for other in sorted(decode.keys()):
                if (decode[other][0] - decode[other][1] ==
                    decode[outcome][0] - decode[outcome][1] and
                    other != outcome):
                    prob += probs[other]
            exp_pts[outcome] += point_dist[1] * prob
            # find results that bring 3rd highest points (same tendency)
            prob = 0
            outc_tend = 0
            if decode[outcome][0] > decode[outcome][1]:
                outc_tend = 1
            elif decode[outcome][0] < decode[outcome][1]:
                outc_tend = 2
            for other in sorted(decode.keys()):
                other_tend = 0
                if decode[outcome][0] > decode[outcome][1]:
                    other_tend = 1
                elif decode[outcome][0] < decode[outcome][1]:
                    other_tend = 2
                if (decode[other][0] - decode[other][1] !=
                    decode[outcome][0] - decode[outcome][1] and
                    other != outcome and
                    outc_tend == other_tend):
                    prob += probs[other]
            exp_pts[outcome] += point_dist[2] * prob
            exp_pts[outcome] = round(exp_pts[outcome], 3)

        best_guess = max(exp_pts, key=exp_pts.get)

        tips[j]['pred'] = decode[best_guess]
        tips[j]['exp_pts'] = exp_pts[best_guess]
        
        print game['teams'][0], game['teams'][1], decode[best_guess], exp_pts[best_guess]
    return tips
            

def submit_guess_for_day(season, day, tips):
    url = "http://openligadb-json.heroku.com/api/\
matchdata_by_group_league_saison?\
league_saison=%s&league_shortcut=bl1&group_order_id=%s" % (season-1, day)
    daydata = urllib2.urlopen(url).read()
    daydata = json.loads(daydata)['matchdata']

    submission = {}
    for game in daydata:
        match_id = int(game['match_id'])
        home_id = int(game['id_team1'])
        for game_dic in tips:
            if game_dic['home_id'] == home_id:
                result_string = (str(game_dic['pred'][0]) + ":" +
                                 str(game_dic['pred'][1]))
                submission[match_id] = result_string
                break

    print 'Uebertrage Tipps an Botliga...'
    print postguess.botliga_post(submission)

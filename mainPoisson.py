import horst

prev_day, prev_season, next_day, this_season = horst.find_prev_and_next_day()

horst.enter_results_for_day(prev_day, prev_season)

horst.update_matchtimes_for_day(this_season, next_day)

horst.update_regressors_for_day(this_season, next_day)

tips, decode = horst.poisson_reg(this_season, next_day)

tips = horst.maximize_expected_points(tips, decode)

horst.submit_guess_for_day(this_season, next_day, tips, 'botligaPoisson')




##- update last day''s results drawing from openliga.
##
##- check if there are any changes in next day''s games drawing from openliga.
##
##- update regressors for next day''s games
##
##- estimate probabilities for next day''s games being careful to model new/onew
##    properly
##
##- use payoff function and estimated probabilities to guess according to
##    maximized expected payoff
##
##- submit guess to API
##
##every once in a while:
##- check and update full season

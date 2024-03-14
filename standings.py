import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

sa = gspread.service_account()
sh = sa.open("SCALPEL RESOURCES")

d_team={1:['The Ass Paddlers (T-1)','Mid Court Crisis (T-2)','Bangstreet Boyz (T-3)', \
    'Team 4 (T-4)','NDYNK (T-5)','Team Body Baggers (T-6)'], 
    2:['Silver Foxes (T-1)','Mighty Gherkins (T-2)','Do It With Relish (T-3)', \
    'Not My Fault (T-4)','Midtown Masters (T-5)','SC Dink Attack (T-6)', \
    'The Motley Krew (T-7)','Mid Court Crises (T-8)','Dinky Time (T-9)'],
    3:['Wonder Women (T-1)','Day Dinkers (T-2)','Jolt (T-3)', \
    'Kitchen Killers (T-4)','The Kitchen Avengers (T-5)','Pickleddink (T-6)', \
    'Free Radicals (T-7)']}

for div in range (1,3):
    schedule_ws = sh.worksheet(f"D{div}.2")

    schedule = get_as_dataframe(schedule_ws,nrows=99)[['Tm A','Tm B','Pts A','Pts B']]

    played = schedule[pd.notna(schedule['Pts A'])]

    dr = {'M':{},'P':{}}
    num_teams = len(d_team[div])
    for n in range(1,1+num_teams):
        dr['M'][n]=[0,0]
        dr['P'][n]=[0,0]

    for m in range(len(played)):
        match = played.iloc[m]

        A,B = match[['Tm A','Tm B']]
        PA,PB = match[['Pts A','Pts B']]
        print(f'm:{m}, match:{match}')
        if PA > PB:
            #print(f'{A} beat {B}')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
        else:
            #print(f'{B} beat {A}')
            dr['M'][A][1]+=1
            dr['M'][B][0]+=1

        #print(f'Team {A} scored {PA} points\nTeam {B} scored {PB} points')
        dr['P'][A]=[dr['P'][A][0]+PA,dr['P'][A][1]+PB]
        dr['P'][B]=[dr['P'][B][0]+PB,dr['P'][B][1]+PA]

    df_standings = pd.DataFrame(Counter(pd.concat([played['Tm A'],played['Tm B']])).items(),columns=['Tnum','MP']).sort_values('Tnum')
    #if div == 1:
    #     df_standings['Team'] = list(set(d_team[1]).symmetric_difference({(d_team[1][2])}))
    #else:
    #    df_standings['Team'] = d_team[div]
    df_standings['Team'] = d_team[div]


    if len(played)==0:
        df_standings['MP']=len(df_standings)*[0]
        df_standings['MW']=len(df_standings)*[0]
        df_standings['ML']=len(df_standings)*[0]
        df_standings['MR']=len(df_standings)*[0.0]

        df_standings['PF']=len(df_standings)*[0]
        df_standings['PA']=len(df_standings)*[0]
        df_standings['PD']=len(df_standings)*[0]
        df_standings['PR']=len(df_standings)*[0.0]
        print('     len(played)==0!!!')
    else:
        df_standings['MW']=[dr['M'][x][0] for x in df_standings.Tnum]
        df_standings['ML']=[dr['M'][x][1] for x in df_standings.Tnum]
        df_standings['MR']=(df_standings.MW/df_standings.MP).round(4)

        df_standings['PF']=[dr['P'][x][0] for x in df_standings.Tnum]
        df_standings['PA']=[dr['P'][x][1] for x in df_standings.Tnum]
        df_standings['PD']=(df_standings.PF-df_standings.PA)
        df_standings['PR']=(df_standings.PF/(df_standings.PF+df_standings.PA)).round(4)
        df_standings['PFm']=(df_standings.PF/(df_standings.MP)).round(4)
        df_standings['PAm']=(df_standings.PA/(df_standings.MP)).round(4)
        df_standings['PDm']=(df_standings.PD/(df_standings.MP)).round(4)
        print(f'df_standings: {df_standings.reset_index(drop=True).to_string()}')
    df_standings["Rank"] = df_standings[['MR','MW','PR','PF']].apply(tuple,axis=1)\
             .rank(method='dense',ascending=False).astype(int)

    df_standings = df_standings.sort_values("Rank")
    df_standings = df_standings[['Rank','Team','MP','MW','ML','MR','PF','PA','PD','PR','PFm','PAm','PDm']]
    print(df_standings.reset_index(drop=True).to_string())

    standings_ws = sh.worksheet(f"S{div}.2")
    set_with_dataframe(standings_ws, df_standings, row=2, col=2)

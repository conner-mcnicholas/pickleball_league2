import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

sa = gspread.service_account()
sh = sa.open("TESTING_SCALPEL RESOURCES")

d_team={1:{1:['Mid Court Crisis (T-1)','Team 2 (T-2)','Bangstreet Boyz (T-3)', \
            'Mighty Gherkins (T-4)','The Ass Paddlers (T-5)','NDYNK (T-6)','Silver Foxes (T-7)'], \
            2:['Wonder Women (T-1)','Team Body Baggers (T-2)','Day Dinkers (T-3)','Do It With Relish (T-4)', \
            'The Motley Krew (T-5)','Not My Fault (T-6)','SC Dink Attack (T-7)','Jolt (T-8)', \
                'Midtown Masters (T-9)'],
             3:['Free Radicals (T-1)','Mid Court Crises (T-2)','Kitchen Killers (T-3)', \
                'Pickleddink (T-4)','The Kitchen Avengers (T-5)','Dinky Time (T-6)']}, \
        2:{1:['The Ass Paddlers (T-1)','Mid Court Crisis (T-2)','Bangstreet Boyz (T-3)', \
            'Team 4 (T-4)','NDYNK (T-5)','Team Body Baggers (T-6)'], 
            2:['Silver Foxes (T-1)','Mighty Gherkins (T-2)','Do It With Relish (T-3)', \
            'Not My Fault (T-4)','Midtown Masters (T-5)','SC Dink Attack (T-6)', \
            'The Motley Krew (T-7)','Mid Court Crises (T-8)','Dinky Time (T-9)'],
            3:['Wonder Women (T-1)','Day Dinkers (T-2)','Jolt (T-3)', \
            'Kitchen Killers (T-4)','The Kitchen Avengers (T-5)','Pickleddink (T-6)', \
            'Free Radicals (T-7)']}}

for r in [1,2]:
    for div in range (1,len(d_team[r])+1):
        schedule_ws = sh.worksheet(f"D{div} Scores.{r}")

        schedule = get_as_dataframe(schedule_ws,nrows=99)[['Tm A','Tm B','Player A1','Player A2','Player B1','Player B2','Pts A','Pts B']]

        played = schedule[pd.notna(schedule['Pts A'])]

        dr = {'M':{},'P':{}}
        num_teams = len(d_team[r][div])
        for n in range(1,1+num_teams):
            dr['M'][n]=[0,0]
            dr['P'][n]=[0,0]

        for m in range(len(played)):
            match = played.iloc[m]

            A,B = match[['Tm A','Tm B']]
            PA,PB = match[['Pts A','Pts B']]

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
        df_standings['Team'] = d_team[r][div]


        if len(played)==0:
            df_standings['MP']=len(df_standings)*[0]
            df_standings['MW']=len(df_standings)*[0]
            df_standings['ML']=len(df_standings)*[0]
            df_standings['MR']=len(df_standings)*[0.0]

            df_standings['PF']=len(df_standings)*[0]
            df_standings['PA']=len(df_standings)*[0]
            df_standings['PD']=len(df_standings)*[0]
            df_standings['PR']=len(df_standings)*[0.0]
            #print('     len(played)==0!!!')
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
            #print(f'df_standings: {df_standings.reset_index(drop=True).to_string()}')
        
        ds = {}
        num_teams = len(d_team[r][div])
        for n in range(1,1+num_teams):
                ds[n]=0

        for t in ["A","B"]:
            for p in [1,2]:
                subteams = played[f'Tm {t}'][played[f'Player {t}{p}'].str.contains('\*')]
                for tsub in subteams:
                    ds[tsub]+=1
                    #print(f'div: {div} --< subteams: {subteams}')

    
        df_standings=pd.merge(df_standings,pd.DataFrame(ds.items(),columns=['Tnum','subs']),on="Tnum")
        df_standings['NS'] = 1-(df_standings['subs']/(df_standings['MP']*2)).round(2)

        df_standings["RANK"] = df_standings[['MR','NS','PR']].apply(tuple,axis=1)\
                .rank(method='dense',ascending=False).astype(int)

        df_standings = df_standings.sort_values("RANK")
        df_standings = df_standings[['RANK','Team','MP','MW','ML','MR','NS','PR','PF','PA','PD','PFm','PAm','PDm']]
        print(df_standings.reset_index(drop=True).to_string())
        print("\n\n")

        standings_ws = sh.worksheet(f"S{div}.{r}")
        set_with_dataframe(standings_ws, df_standings, row=2, col=2)

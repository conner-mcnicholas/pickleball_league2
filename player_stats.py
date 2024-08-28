import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

sa = gspread.service_account()
sh = sa.open("SCALPEL Ladder")


for div in range (1,3):
    schedule_ws = sh.worksheet(f"D{div}_Scores")
    schedule = get_as_dataframe(schedule_ws,nrows=100)[['Player A1', \
        'Player A2','Player B1','Player B2','Pts A','Pts B']]
    played = schedule[pd.notna(schedule['Pts A'])]

    if len(played) == 0:
        break

    players_ws = sh.worksheet("Players")
    df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).PLAYER).sum()) \
        [['PLAYER','DIV','SKILL','AGE','EXP','GEN']]
    df_players = df_players[df_players.DIV == div]
    
    players = list(df_players.PLAYER)
    
    dr={'M':{},'P':{}}
    for p in players:
        for k in ['M','P']:
            dr[k][p]=[0,0]

    allplayers = list(pd.concat([played['Player A1'].str.strip(),played['Player A2'].str.strip(), \
                                 played['Player B1'].str.strip(),played['Player B2'].str.strip()]))
    df_stats = pd.DataFrame(Counter(allplayers).items(),columns=['PLAYER','GP']).sort_values('PLAYER').reset_index(drop=True)
    df_stats = pd.merge(left=df_players[['PLAYER','SKILL']],right=df_stats,how='outer').fillna(0)

    for m in range(len(played)):
        match = played.iloc[m]
  
        A1,A2,B1,B2 = match[['Player A1','Player A2','Player B1','Player B2']].str.strip()
        PA,PB = match[['Pts A','Pts B']]
        
        #print(f'Pts A:{Pts A},Pts B:{Pts B}')
        if PA > PB:
            #print(f'{A} beat {B} by score:{PA}-{PB}')
            dr['M'][A1][0]+=1
            dr['M'][B1][1]+=1
            
            dr['M'][A2][0]+=1
            dr['M'][B2][1]+=1
        else:
            #print(f'{B} beat {A}by score:{PB}-{PA}')
            dr['M'][A1][1]+=1
            dr['M'][B1][0]+=1

            dr['M'][A2][1]+=1
            dr['M'][B2][0]+=1
      
        dr['P'][A1]=[dr['P'][A1][0]+PA,dr['P'][A1][1]+PB]
        dr['P'][B1]=[dr['P'][B1][0]+PB,dr['P'][B1][1]+PA]

        dr['P'][A2]=[dr['P'][A2][0]+PA,dr['P'][A2][1]+PB]
        dr['P'][B2]=[dr['P'][B2][0]+PB,dr['P'][B2][1]+PA]

        if len(played)==0:
            df_stats['GP']=len(df_stats)*[0]
            df_stats['W']=len(df_stats)*[0]
            df_stats['L']=len(df_stats)*[0]
            df_stats['WR']=len(df_stats)*[0.0]

            df_stats['PF']=len(df_stats)*[0]
            df_stats['PA']=len(df_stats)*[0]
            df_stats['PD']=len(df_stats)*[0]
            df_stats['PFm']=len(df_stats)*[0.0]
            df_stats['PAm']=len(df_stats)*[0.0]
            df_stats['PDm']=len(df_stats)*[0.0]
            df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)

        else:
            df_stats = df_stats[pd.notna(df_stats.PLAYER)]
            df_stats['W']=[dr['M'][x][0] for x in df_stats.PLAYER]
            df_stats['L']=[dr['M'][x][1] for x in df_stats.PLAYER]
            df_stats['WR']=(df_stats.W/df_stats.GP).round(4)

            df_stats['PF']=[dr['P'][x][0] for x in df_stats.PLAYER]
            df_stats['PA']=[dr['P'][x][1] for x in df_stats.PLAYER]
            df_stats['PD']=(df_stats.PF-df_stats.PA)
            df_stats['PFm']=(df_stats.PF/(df_stats.GP)).round(4)
            df_stats['PAm']=(df_stats.PA/(df_stats.GP)).round(4)
            df_stats['PDm']=(df_stats.PD/(df_stats.GP)).round(4)
            df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)
            
    df_stats["#"] = df_stats[['W','WR','PR','PF']].apply(tuple,axis=1)\
        .rank(method='min',ascending=False).astype(int)
    df_stats = df_stats.sort_values("#")
    df_stats = df_stats[['#','PLAYER','GP','W','L','WR','PF','PA','PD','PR','PFm','PAm','PDm']]
    print(df_stats.reset_index(drop=True).to_string())

    #df_stats_tophalf = df_stats.head(int(np.floor((len(df_players)/2))))
    stats_ws = sh.worksheet(f"A{div}")
    set_with_dataframe(stats_ws, df_stats.fillna(0), row=3, col=2)

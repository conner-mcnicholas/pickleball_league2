import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

sa = gspread.service_account()
sh = sa.open("SCALPEL Ladder")

<<<<<<< HEAD
=======
subs = {1:["Jack-Sub*","noshow-sub*","nofault-sub"],2:["Colleen-Sub*","Diego-Sub*","noshow-sub*","nofault-sub"],3:["noshow-sub*","nofault-sub"]}
>>>>>>> parent of ab9f9e0 (round robin bifercate in testing)

for div in range (1,3):
    schedule_ws = sh.worksheet(f"D{div} Results")
    schedule = get_as_dataframe(schedule_ws,nrows=100)[['Player A1', \
        'Player A2','Player B1','Player B2','Pts A','Pts B']]
    played = schedule[pd.notna(schedule['Pts A'])]

    if len(played) == 0:
        break

    players_ws = sh.worksheet("Players")
    df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).PLAYER).sum()) \
<<<<<<< HEAD
        [['PLAYER','D','RAT','AGE','EXP','GEN']]
    df_players = df_players[df_players.D == div]
    
    players = list(df_players.PLAYER)
=======
        [['DIVn','TEAMn','PLAYER','SKILL','AGE','EXP','GEN','CAP']]
    df_players = df_players[df_players.DIVn == div]
    players = list(df_players.PLAYER)
    players.append("nofault-sub")
    players.append("noshow-sub*")
    players.append("?")
    players.append("Colleen-Sub*")
    players.append("Diego-Sub*")
    players.append("Julie-Sub*")
    players.append("Jack-Sub*")
    
>>>>>>> parent of ab9f9e0 (round robin bifercate in testing)
    
    dr={'M':{},'P':{}}
    for p in players:
        for k in ['M','P']:
            dr[k][p]=[0,0]

<<<<<<< HEAD
    allplayers = list(pd.concat([played['Player A1'].str.strip(),played['Player A2'].str.strip(), \
                                 played['Player B1'].str.strip(),played['Player B2'].str.strip()]))
    df_stats = pd.DataFrame(Counter(allplayers).items(),columns=['PLAYER','GP']).sort_values('PLAYER').reset_index(drop=True)
    df_stats = pd.merge(left=df_players[['PLAYER','RAT']],right=df_stats,how='outer').fillna(0)
=======
    allplayers = []
    for p in ['Player A1','Player A2','Player B1','Player B2']:
        for pi in list(played[p]):
            allplayers.append(pi)
    df_stats = pd.DataFrame(Counter(allplayers).items(),columns=['PLAYER','MP']).sort_values('PLAYER').reset_index(drop=True)
    df_stats = pd.concat([df_stats,df_players['SKILL']],axis=1)[['PLAYER','SKILL','MP']]
>>>>>>> parent of ab9f9e0 (round robin bifercate in testing)

    for m in range(len(played)):
        match = played.iloc[m]
  
        A1,A2,B1,B2 = match[['Player A1','Player A2','Player B1','Player B2',]]
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
            df_stats['PD\'']=len(df_stats)*[0.0]

        else:
            df_stats = df_stats[pd.notna(df_stats.PLAYER)]
            df_stats['W']=[dr['M'][x][0] for x in df_stats.PLAYER]
            df_stats['L']=[dr['M'][x][1] for x in df_stats.PLAYER]
            df_stats['WR']=(df_stats.W/df_stats.GP).round(4)

            df_stats['PF']=[dr['P'][x][0] for x in df_stats.PLAYER]
            df_stats['PA']=[dr['P'][x][1] for x in df_stats.PLAYER]
<<<<<<< HEAD
            df_stats['PD\'']=((df_stats.PF-df_stats.PA)/df_stats.GP).round(4)

    df_stats = df_stats[df_stats.PLAYER !='George Propper']
    if div == 2:
        df_stats = df_stats[df_stats.PLAYER !='Mauricio Cuervo']           
    df_stats.sort_values(['WR', 'PF','PD\''], ascending = [False, False,False], na_position ='last',inplace=True)
    df_stats['#'] = range(1,len(df_stats)+1)
    df_stats = df_stats[['#','PLAYER','GP','W','L','WR','PF','PA','PD\'']]
    df_stats.loc[df_stats.GP==0,['GP','W','L','WR','PF','PA','PD\'']]=""

=======
            df_stats['PD']=(df_stats.PF-df_stats.PA)
            df_stats['PFm']=(df_stats.PF/(df_stats.MP)).round(4)
            df_stats['PAm']=(df_stats.PA/(df_stats.MP)).round(4)
            df_stats['PDm']=(df_stats.PD/(df_stats.MP)).round(4)
            df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)
            
    df_stats = df_stats[~df_stats['PLAYER'].isin(subs[div])]
    df_stats["RANK"] = df_stats[['MR','MW','PR','PF']].apply(tuple,axis=1)\
        .rank(method='min',ascending=False).astype(int)
    df_stats = df_stats.sort_values("RANK")
    df_stats = df_stats[['RANK','PLAYER','MP','MW','ML','MR','PF','PA','PD','PR','PFm','PAm','PDm']]
>>>>>>> parent of ab9f9e0 (round robin bifercate in testing)
    print(df_stats.reset_index(drop=True).to_string())

    stats_ws = sh.worksheet(f"Leaderboard")
    set_with_dataframe(stats_ws, df_stats, row=2, col=2+((div-1)*10))

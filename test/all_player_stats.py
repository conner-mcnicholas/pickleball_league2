import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

sa = gspread.service_account()
sh = sa.open("TESTING_SCALPEL RESOURCES")

subs = {1:{1:["Dave-R","Evan"],2:["Dave-J","Carina"],3:["Julie"]}, \
        2:{1:["Jack"],2:["Colleen","Diego","Alessandro","Jax"],3:["Julie"]}}

for r in [1,2]:

    dfl = []

    for div in range (1,4):
        schedule_ws = sh.worksheet(f"D{div} Scores.{r}")
        schedule = get_as_dataframe(schedule_ws,nrows=100)[['Tm A','Tm B','Player A1', \
            'Player A2','Player B1','Player B2','Pts A','Pts B']]
        played = schedule[pd.notna(schedule['Pts A'])]

        if len(played) == 0:
            break

        players_ws = sh.worksheet(f"Player Info.{r}")
        df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).PLAYER).sum()) \
            [['DIVn','TEAMn','PLAYER','SKILL','AGE','EXP','GEN','CAP']]
        df_players = df_players[df_players.DIVn == div]
        
        subs[r][div]=pd.concat([pd.Series(["noshow-sub*","nofault-sub"]),pd.Series([f"{x}-Sub*" for x in subs[r][div]])])

        players = list(pd.concat([df_players.PLAYER,pd.Series(subs[r][div])]))
        
        dr={'M':{},'P':{}}
        for p in players:
            for k in ['M','P']:
                dr[k][p]=[0,0]

        allplayers = list(pd.concat([played['Player A1'].str.strip(),played['Player A2'].str.strip(), \
                                    played['Player B1'].str.strip(),played['Player B2'].str.strip()]))
        df_stats = pd.DataFrame(Counter(allplayers).items(),columns=['PLAYER','MP']).sort_values('PLAYER').reset_index(drop=True)
        df_stats = pd.concat([df_stats,df_players['SKILL']],axis=1)[['PLAYER','SKILL','MP']]

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
                df_stats['MP']=len(df_stats)*[0]
                df_stats['MW']=len(df_stats)*[0]
                df_stats['ML']=len(df_stats)*[0]
                df_stats['MR']=len(df_stats)*[0.0]

                df_stats['PF']=len(df_stats)*[0]
                df_stats['PA']=len(df_stats)*[0]
                df_stats['PD']=len(df_stats)*[0]
                df_stats['PFm']=len(df_stats)*[0.0]
                df_stats['PAm']=len(df_stats)*[0.0]
                df_stats['PDm']=len(df_stats)*[0.0]
                df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)

            else:
                df_stats = df_stats[pd.notna(df_stats.PLAYER)]
                df_stats['MW']=[dr['M'][x][0] for x in df_stats.PLAYER]
                df_stats['ML']=[dr['M'][x][1] for x in df_stats.PLAYER]
                df_stats['MR']=(df_stats.MW/df_stats.MP).round(4)

                df_stats['PF']=[dr['P'][x][0] for x in df_stats.PLAYER]
                df_stats['PA']=[dr['P'][x][1] for x in df_stats.PLAYER]
                df_stats['PD']=(df_stats.PF-df_stats.PA)
                df_stats['PFm']=(df_stats.PF/(df_stats.MP)).round(4)
                df_stats['PAm']=(df_stats.PA/(df_stats.MP)).round(4)
                df_stats['PDm']=(df_stats.PD/(df_stats.MP)).round(4)
                df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)

        df_stats = df_stats[~df_stats['PLAYER'].isin(subs[r][div])]
        df_stats["RANK"] = df_stats[['MW','MR','PR','PF']].apply(tuple,axis=1)\
            .rank(method='min',ascending=False).astype(int)
        df_stats = df_stats.sort_values("RANK")
        df_stats = df_stats[['RANK','PLAYER','MP','MW','ML','MR','PF','PA','PD','PR','PFm','PAm','PDm']]
        #print(df_stats.reset_index(drop=True).to_string())

        #df_stats_tophalf = df_stats.head(int(np.floor((len(df_players)/4))))
        stats_ws = sh.worksheet(f"A{div}.{r}")
        #set_with_dataframe(stats_ws, df_stats_tophalf, row=3, col=2)
        set_with_dataframe(stats_ws, df_stats, row=3, col=2)
        dfl.append(df_stats)

    df_all = pd.concat([dfl[x] for x in range(div)])
    #df_all = pd.concat([dfl[0],dfl[1],dfl[2]])
    divl=[]
    for i in range(div):
        for j in range(1,len(dfl[i])+1):
            divl.append(i+1)
    df_all['DIV'] = divl
    df_all["RANK"] = df_all[['MR','MW','PR','PF']].apply(tuple,axis=1)\
        .rank(method='min',ascending=False).astype(int)
    df_all = df_all.sort_values("RANK")
    df_all = df_all[['RANK','PLAYER','DIV','MP','MW','ML','MR','PF','PA','PD','PR','PFm','PAm','PDm']]
    print(df_all.reset_index(drop=True).to_string())
    all_stats_ws = sh.worksheet(F"ALL PLAYERS.{r}")
    set_with_dataframe(all_stats_ws, df_all, row=1, col=2)


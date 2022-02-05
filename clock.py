#====================== Scheduler Code ==================
import os
import tweepy as tw
import pandas as pd
import re
import numpy as np
from textblob import TextBlob
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
from wordcloud import WordCloud
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine,MetaData,Table

#Function to process the text 
def cleaning_sentence(sentence):
        # Remove all the special characters
        processed_feature = re.sub(r'\W', ' ', str(sentence))
        
        # remove all single characters
        processed_feature= re.sub(r'\s+[a-zA-Z]\s+', ' ', processed_feature)
        
        #remove all digit in the characters
        processed_feature = re.sub(" \d+", " ", processed_feature)
        
        #Remove single characters from the start
        processed_feature = re.sub(r'\^[a-zA-Z]\s+', ' ', processed_feature) 
        
        # Substituting multiple spaces with single space
        processed_feature = re.sub(r'\s+', ' ', processed_feature, flags=re.I)
        
        # Removing prefixed 'b'
        processed_feature = re.sub(r'^b\s+', '', processed_feature)
        
        # Converting to Lowercase
        processed_feature = processed_feature.lower()

        p=processed_feature.split(' ')
    
        processed_feature=''
        for i in p:
            if i.startswith('https') or i.startswith('@') or len(i) <5:
                continue
                
            processed_feature=processed_feature + ' ' + i
        
        return processed_feature

#Define Scheduler 
sched=BlockingScheduler()

#Function to extract tweet and save in Database
def extracttweets():
    print('Extracting Tweets !!!')

    #Extract the musician list
    musicians=pd.read_csv('musician.txt')
    musicians=list(musicians['Musicians'])
    musicians

    #Twitter API credentials
    API_Key=''
    API_Secret_Key=''
    Bearer_Token=''

    Access_token=''
    Access_token_secret=''

    #Authentication for api
    auth=tw.OAuthHandler(API_Key,API_Secret_Key)
    auth.set_access_token(Access_token, Access_token_secret)
    api=tw.API(auth,wait_on_rate_limit=True)

    #Extract the today time and yesterday time
    today=datetime.now()
    today=today.replace(microsecond=0)
    yesterday=today-timedelta(days=1,hours=0)

    #Exract tweet for the period of 24 hours
    ave_sentiment=[]
    alltweets=[]
    musics=[]
    Time=[]
    for musician in musicians:
        Tweets=tw.Cursor(api.search,q="#"+musician,
                                   lang='en',
                                   since=yesterday.date(),
                                   until=today.date()).items(100)
        text=[]
        for tweet in Tweets:
            
                Time.append(tweet.created_at)
                t=tweet.text.encode('utf-8')
                txt=cleaning_sentence(t.decode('utf8'))
                text.append(txt)
                alltweets.append(txt)
        
        if len(text)==0:
            continue
        
        sentiments=[]
        for txt in text:
            textB=TextBlob(txt)
            sentiments.append(textB.sentiment.polarity)
            
        ave_sentiment.append(sum(sentiments)/len(sentiments))
        musics.append(musician)

    time=yesterday-timedelta(hours=23)
    times=[]
    positives=[]
    negatives=[]
    neutrals=[]

    #Compute the number of positive, negative and neutral tweets 
    while (today-time).total_seconds()>0:
        p=0
        ng=0
        nu=0
            
        for i in range(len(Time)):
            if str(Time[i])==str(time):
                textB=TextBlob(alltweets[i])
                sen=textB.sentiment.polarity
                    
                if sen <0.0:
                   ng=ng+1      
                elif sen >=0.0 and sen <0.5:
                   nu=nu+1   
                else:  
                   p=p+1 
        if (p+ng+nu)>0:    
           positives.append(100*p/(p+ng+nu))
           negatives.append(33*ng/(p+ng+nu))
           neutrals.append(50*nu/(p+ng+nu))
        else:
            positives.append(0)
            negatives.append(0)
            neutrals.append(0)
        times.append(time)
        time=time+timedelta(seconds=1)

    df=pd.DataFrame(data={'Time':times,'Positive':positives,'Negative':negatives,'Neutral':neutrals})
    df=df.set_index('Time')

    #Classify the class by sentimental score
    sentiments_score=[]
    sentiments=[]
    for tweet in alltweets:
        textB=TextBlob(tweet)
        sen=textB.sentiment.polarity*100
        sentiments_score.append(abs(sen))
        if sen <0.0:
            sentiments.append('Negative')
            
        elif sen >=0.0 and sen <0.6:
            sentiments.append('Neutral')   
        
        else:  
             sentiments.append('Positive')

    combinetxt=''
    for i in alltweets:
        combinetxt+=i[3:]

    combinetxt=pd.DataFrame(data={'combine text': combinetxt.split(' ')})
    
    d=pd.DataFrame(data={'score':sentiments_score,'sentiment':sentiments})

    engine=create_engine('postgres://xgqsmauzbfhupd:ca4a90e4c15aa8751194f2663079ae5b007530756f4b922f11caf6ae13196cb4@ec2-54-166-167-192.compute-1.amazonaws.com:5432/d93b9hdlt7c4m',connect_args={'sslmode':'require'},echo=True)
  
    dbConnection    = engine.connect();

    #Save sentimental analytic results in form tables in the Postgre Database
    try:
            df.to_sql('Sentiment', dbConnection)
    except:
            meta = MetaData()
            table_to_drop = Table('Sentiment', 
                                       meta, autoload=True, autoload_with=engine)
            
            table_to_drop.drop(engine)
            df.to_sql('Sentiment', dbConnection)

    try:
            d.to_sql('Data', dbConnection)
    except:
            meta = MetaData()
            table_to_drop = Table('Data', 
                                       meta, autoload=True, autoload_with=engine)
            
            table_to_drop.drop(engine)
            d.to_sql('Data', dbConnection)
            
    Result=pd.DataFrame(data={'Musician':musics,'Sentiment Score':ave_sentiment})

    
    try:
            Result.to_sql('Result', dbConnection)
    except:
            meta = MetaData()
            table_to_drop = Table('Result', 
                                       meta, autoload=True, autoload_with=engine)
            
            table_to_drop.drop(engine)
            Result.to_sql('Result', dbConnection)

    try:
            combinetxt.to_sql('combinetxt', dbConnection)
    except:
            meta = MetaData()
            table_to_drop = Table('combinetxt', 
                                       meta, autoload=True, autoload_with=engine)
            
            table_to_drop.drop(engine)
            combinetxt.to_sql('combinetxt', dbConnection)
            
    print('Saving Tweets  !!!')
    print(Result)

    #Close connection
    dbConnection.close()    

extracttweets()
sched.add_job(extracttweets,'interval',hours=2)
sched.start()

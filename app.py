#======================= App Code===============
import pandas as pd
import os
import dash
import base64
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output,State
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from io import BytesIO
from wordcloud import WordCloud

external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css']
app=dash.Dash()

server = app.server

#Connect to the server (Postgres Database on Heroku)
engine=create_engine('url',connect_args={'sslmode':'require'},echo=True)
dbConnection    = engine.connect();

#set up the styling the Dashboard
app.css.config.serve_locally=True
app.scripts.config.serve_locally=True
app.config['suppress_callback_exceptions']=True
colors = {
    'background': '#FFFFFF',
    'text': '#1FA5FC'
}

#Load the sentimental analytics tables from the database
sentiment_table=pd.read_sql("select * from \"Sentiment\"",dbConnection)
df_long=pd.melt(sentiment_table, id_vars=['Time'])
df_long=df_long[df_long['value']>0]
sentiments=pd.read_sql("select * from \"Data\"",dbConnection)
dr=pd.read_sql("select * from \"Result\"", dbConnection)
combinetxt=pd.read_sql("select * from \"combinetxt\"", dbConnection)

#post process the analytics result for displaying on dashboard
#Creating Wordcloud
img= BytesIO()
combine_text=list(combinetxt['combine text'])
combinetxt=''
for c in combine_text:
    combinetxt=combinetxt+ ' '+ c
wc=WordCloud(background_color = "black").generate(combinetxt)

(wc.to_image()).save(img, format='PNG')

#Processing the other Tables for Visualization 
df_long.columns=['Time','sentiments','percentage']
dmusic=dr.sort_values('Sentiment Score',ascending=False)
dmusic=dmusic.iloc[:10,:]  #Extract the top 10 musicians based on the tweets 

df=sentiments['sentiment'].value_counts()
values=list(df)
labels=list(df.index)


#======================================Dashboard code for sentimental analytic (Area Graph, Ring Pie Chart, Bar chart and Word Cloud  ================================
app.layout = html.Div(style={'backgroundColor': colors['background'],'display':'inline-block'}, children=[
    html.H1(
        children='Sentimental Analaysis for Ranking Nigeria Musician Based on Tweets',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }),
        dcc.Graph(id="graph",style={'width':'70%'},
                  figure = px.area(df_long,x='Time',y='percentage',color='sentiments',
                                   title='Sentimental Analysis on the Last 24 Hours Tweets')),
    
        dcc.Graph(id="graph2",style={'width':'30%','float': 'right','margin-top':'-400px','margin-right':'-200px'},
                  figure=go.Figure(data=[go.Pie(labels=labels, values=values, hole=.5)])),

        html.Div(children=[
        dcc.Graph(id="graph3",style={'width':'70%'},
                  figure = px.bar(dmusic,x='Sentiment Score',y='Musician', color='Musician',
                                   title="Top Ranked Musicians Based on Sentiments From Tweets")),
        html.Div(style={'float':'right','margin-top':'-400px','margin-right':'-500px'},
                 children=[
                     html.H4('        Most Frequently Used Word (in WordCloud)'),
                     html.Img(src='data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode()),height=350)])
        ])   
        ])

#Close the connection to the database       
dbConnection.close()

#Run App     
if __name__=='__main__':
    app.run_server(debug=True)

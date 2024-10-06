from googleapiclient.discovery import build
import pymongo
import streamlit as st
import pandas as pd
import psycopg2

# api key

def api_connect():
    api_id = "AIzaSyB1KbXPECGWhvMIXK6UT1D3JtTfnLa7nhI"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name,api_version,developerKey=api_id)
    return youtube

youtube = api_connect()
    
# getting channel information

def channel_info(channel_id):      
    request = youtube.channels().list(
        part = "statistics,snippet,contentDetails",
        id = channel_id
        
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_id = i['id'],
                    channel_title = i['snippet']['title'],
                    channel_viewcount = i['statistics']['viewCount'],
                    channel_subscribers = i['statistics']['subscriberCount'],
                    channel_videocount = i['statistics']['videoCount'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads']
                    )
    return data

def get_video_ids(channel_id):
     video_ids = []
     request = youtube.channels().list(
          part = "contentDetails",
          id = channel_id,
          
     )
     response = request.execute()

     playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

     next_page_token = None
     while True:
          response1 = youtube.playlistItems().list(
          part = "snippet",
          playlistId = playlist_id,
          maxResults = 50,
          pageToken = next_page_token
          ).execute()

          for i in range(len(response1['items'])):
               video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
          next_page_token = response1.get("nextPageToken")
          if next_page_token is None:
               break
          
     return video_ids     
 
# getting video information
def get_video_info(video_ids):
    video_info = []
    for video_id in video_ids:
        response = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_id
            
        ).execute()

        for i in response["items"]:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                    Channel_Id=i['snippet']['channelId'],
                    Video_Id=i['id'],
                    Title=i['snippet']['title'],
                    Tags=i['snippet'].get('tags'),
                    Thumbnail=i['snippet']['thumbnails']['default']['url'],
                    Description=i['snippet'].get('description'),
                    Published_Date=i['snippet']['publishedAt'],
                    Duration=i['contentDetails']['duration'],
                    Views=i['statistics'].get('viewCount'),
                    Likes=i['statistics'].get('likeCount'),
                    Comments=i['statistics'].get('commentCount'),
                    Favorite_Count=i['statistics']['favoriteCount'],
                    Definition=i['contentDetails']['definition'],
                    Caption_Status=i['contentDetails']['caption']
                    )
            video_info.append(data)
    return video_info

# getting comment information
def comment_information(videos_ids):
    comment_info = []
    try:
        for cmt_details in videos_ids:
            response = youtube.commentThreads().list(
                            part = "id,replies,snippet",
                            videoId = cmt_details,
                            maxResults = 50    
                ).execute()

            for i in response['items']:
                data = dict(comment_id = i['snippet']['topLevelComment']['id'],
                            comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_publishedat = i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_info.append(data)
    except:
        pass
    return comment_info

# getting playlist  details
def get_playlist_details(channel_id):
    playlist_data = []
    next_page_token = None
    try:
        while True:
            response = youtube.playlists().list(
                            part = 'snippet,contentDetails',
                            channelId = channel_id,
                            maxResults = 50,
                            pageToken = next_page_token
            ).execute()

            for i in response['items']:
                data = dict(playlist_id = i['id'],
                            channels_id = i['snippet']['channelId'],
                            channel_title = i['snippet']['channelTitle'],
                            published_at = i['snippet']['publishedAt'],
                            item_count = i['contentDetails']['itemCount']
                            )
                playlist_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return playlist_data

# mongo db uploading

client = pymongo.MongoClient("mongodb+srv://naren_1995:narendran@cluster0.unvoqct.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['youtube_data']

def channel_details(channel_id):
    ch_details = channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_info = get_video_info(vi_ids)
    cmt_details = comment_information(vi_ids)
    
    col1 = db["channel_details"]
    col1.insert_one({"channel_details":ch_details,"playlist_details":pl_details,
                     "video_details":vi_info,"comment_details":cmt_details})
    
    return "upload success"

# table creatioon for channels
def channel_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "16Dec@1995",
                            database = "youtube_data",
                            port = 5432)
                            
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(channel_id varchar(200) primary key,
                                            channel_title varchar(150),
                                            channel_viewcount bigint,
                                            channel_subscribers bigint,
                                            channel_videocount int,
                                            playlist_id varchar(150))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table details have already created")
        
    # data taken from mongodb and converted into dataframe
    ch_data = []
    db = client['youtube_data']
    col1 = db["channel_details"]
    for ch_dtls in col1.find({},{"_id":0,"channel_details":1}):
        ch_data.append(ch_dtls['channel_details'])
    df_ch = pd.DataFrame(ch_data)
                                    
    for index,row in df_ch.iterrows():
        insert_query = '''insert into channels(channel_id,
                                            channel_title,
                                            channel_viewcount,
                                            channel_subscribers,
                                            channel_videocount,
                                            playlist_id)
        
                                            values(%s,%s,%s,%s,%s,%s)'''
        values =(row['channel_id'],
                row['channel_title'],
                row['channel_viewcount'],
                row['channel_subscribers'], 
                row['channel_videocount'], 
                row['playlist_id'])
        
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("channel values have already created")
        

# table creation for playlists

def playlist_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "16Dec@1995",
                            database = "youtube_data",
                            port = 5432)
                            
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(playlist_id varchar(200) primary key,
                                                            channels_id varchar(200),
                                                            channel_title varchar(150),
                                                            published_at timestamp,
                                                            item_count int)'''
                                                            
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("playlsit values have already entered")
        
    # data taken from mongodb and converted into dataframe
    pl_data = []
    db = client["youtube_data"]
    col1 = db["channel_details"]
    for pl_list in col1.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_list['playlist_details'])):
            pl_data.append(pl_list['playlist_details'][i])
    df_pl = pd.DataFrame(pl_data)     


    for index,row in df_pl.iterrows():
        insert_query = '''insert into playlists(playlist_id,
                                                channels_id,
                                                channel_title,
                                                published_at,
                                                item_count)
                                                
                                                values(%s,%s,%s,%s,%s)'''
                                                
        values = (row['playlist_id'],
                row['channels_id'],
                row['channel_title'],
                row['published_at'],
                row['item_count'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Playlist details have already entered")
                                                
# table creation for videos
def videos_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "16Dec@1995",
                            database = "youtube_data",
                            port = 5432)
                            
    cursor = mydb.cursor()


    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    try:
        insert_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(200),
                                                    Video_Id varchar(150) primary key,
                                                    Title varchar(250),
                                                    Tags text,
                                                    Thumbnail varchar(250),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(100),
                                                    Caption_Status varchar(10))'''

        cursor.execute(insert_query)
        mydb.commit()
    except:
        print("videos information have already inserted")

    vi_data = []
    db = client['youtube_data']
    col1 = db['channel_details']
    for vi_dts in col1.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_dts['video_details'])):
            vi_data.append(vi_dts['video_details'][i])
    df_vi = pd.DataFrame(vi_data)

    for index,row in df_vi.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                    Channel_Id,
                                    Video_Id,
                                    Title,
                                    Tags,
                                    Thumbnail,
                                    Description,
                                    Published_Date,
                                    Duration,
                                    Views,
                                    Likes,
                                    Comments,
                                    Favorite_Count,
                                    Definition,
                                    Caption_Status)
                                    
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                    
        values =(row["Channel_Name"],
                row["Channel_Id"],
                row["Video_Id"],
                row["Title"],
                row["Tags"],
                row["Thumbnail"],
                row["Description"],
                row["Published_Date"],
                row["Duration"],
                row["Views"],
                row["Likes"],
                row["Comments"],
                row["Favorite_Count"],
                row["Definition"],
                row["Caption_Status"])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("videos information have already inserted")


def comments_table():
    # Establish a connection to PostgreSQL
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="16Dec@1995",
        database="youtube_data",
        port=5432
    )
    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()
    

    # Create the comments table
    create_query = '''
    CREATE TABLE comments (
        comment_id VARCHAR(100) PRIMARY KEY,
        comment_text TEXT,
        comment_author VARCHAR(100),
        comment_publishedat TIMESTAMP
    )
    '''
    cursor.execute(create_query)
    mydb.commit()
    

    # Fetch comments from MongoDB
    
    db = client['youtube_data']
    col1 = db['channel_details']
    
    comment_data = []
    for cmt_dtls in col1.find({}, {"_id": 0, "comment_details": 1}):
        for comment in cmt_dtls.get('comment_details', []):
            comment_data.append(comment)
    
    df_cmt = pd.DataFrame(comment_data)

    # Insert comments into PostgreSQL
    for index, row in df_cmt.iterrows():
        insert_query = '''
        INSERT INTO comments (comment_id, comment_text, comment_author, comment_publishedat)
        VALUES (%s, %s, %s, %s)'''
        values = (
            row['comment_id'],
            row['comment_text'],
            row['comment_author'],
            row['comment_publishedat']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            
        except Exception as e:
            print("not success")

    # Close cursor and connection
    cursor.close()
    mydb.close()

def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Table created sucessfully"

def strm_channel(): 
    ch_data = []
    db = client['youtube_data']
    col1 = db["channel_details"]
    for ch_dtls in col1.find({},{"_id":0,"channel_details":1}):
        ch_data.append(ch_dtls['channel_details'])
    df_chs = st.dataframe(ch_data)
    return df_chs

def strm_playlists():    
    pl_data = []
    db = client["youtube_data"]
    col1 = db["channel_details"]
    for pl_list in col1.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_list['playlist_details'])):
            pl_data.append(pl_list['playlist_details'][i])
    df_pls = st.dataframe(pl_data)   
    return df_pls

def strm_videos():
    vi_data = []
    db = client['youtube_data']
    col1 = db['channel_details']
    for vi_dts in col1.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_dts['video_details'])):
            vi_data.append(vi_dts['video_details'][i])
    df_vis = st.dataframe(vi_data)
    return df_vis

def strm_comments():
    db = client['youtube_data']
    col1 = db['channel_details']
    comment_data = []
    for cmt_dtls in col1.find({}, {"_id": 0, "comment_details": 1}):
        for comment in cmt_dtls.get('comment_details', []):
            comment_data.append(comment)
    df_cmts = st.dataframe(comment_data)
    return df_cmts
            
# streamlit part

with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.header("Key Learnings")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id = st.text_input("Enter the channel ID")
if st.button("collect and store data"):
    channel_ids = []
    db = client["youtube_data"]
    col1 = db["channel_details"]
    for ch_data in col1.find({},{"_id":0,"channel_details":1}):
        channel_ids.append(ch_data['channel_details']['channel_id'])
        
    if channel_id in channel_ids:
        st.success("channel details already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to sql"):
    Tables = tables()
    st.success(Tables)
show_table = st.radio("SELECT THE TABLE FOR VIEW",("channels","playlists","videos","comments"))
if show_table == "channels":
    strm_channel()
if show_table == "playlists":
    strm_playlists()
if show_table == "videos":
    strm_videos()
if show_table == "comments":
    strm_comments()
# postrgresql connection
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="16Dec@1995",
    database="youtube_data",
    port=5432
)
cursor = mydb.cursor()
question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
if question == "1. All the videos and the channel name":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1,columns=['video name','channel name'])
    st.write(df1)
    
elif question == "2. channels with most number of videos":
    query2 = '''select channel_title as channel_name,channel_videocount as video_count from channels order by channel_videocount desc '''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=['channel name','video count'])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''select views as video_views,channel_name as channelname,title as video_title from videos order by views desc limit(10)'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=['video views','channel name','video count'])
    st.write(df3)
    
elif question== "4. comments in each videos":
    query4 = '''select channel_name as channelname,title as video_title,comments as comment_count from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns=['channel name','video title','comment count'])
    st.write(df4)

elif question== "5. Videos with higest likes":
    query5 = '''select channel_name as channelname,title as video_title,likes as videos_likes from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=['channel name','video title','likes count'])
    st.write(df5)
    
elif question== "5. Videos with higest likes":
    query5 = '''select channel_name as channelname,title as video_title,likes as videos_likes from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=['channel name','video title','likes count'])
    st.write(df5)
    
elif question== "6. likes of all videos":
    query6 = '''select channel_name as channelname,title as video_title,likes as videos_likes from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns=['channel name','video title','likes count'])
    st.write(df6)
    
elif question== "7. views of each channel":
    query7 = '''select channel_title as channeltitle,channel_viewcount as channels_views from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns=['channel name','channel views'])
    st.write(df7)
    
elif question== "8. videos published in the year of 2022":
    query8 =  '''select channel_name as channelname,title as video_title,published_date as publishdate from videos where extract (year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=['channel name','video title','published date'])
    st.write(df8)
    
elif question== "9. average duration of all videos in each channel":
    query9 =  '''select channel_name as channelname,title as video_title,duration as avg_duration from videos'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns=['channel name','video title','average duration'])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channel name"]
        video_title = row['video title']
        average_duration=row["average duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,videotitle = video_title ,avgduration=average_duration_str))
    df09=pd.DataFrame(T9)
    st.write(df09)
    
elif question== "10. videos with highest number of comments":
    query10 =  '''select channel_name as channelname,title as video_title,comments as comment from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10,columns=['channel name','video title','published date'])
    st.write(df10)
    

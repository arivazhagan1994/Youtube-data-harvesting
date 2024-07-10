import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pandas as pd
from itertools import islice
import mysql.connector
from mysql.connector import Error
from googleapiclient.errors import HttpError
import datetime

st.set_page_config(
    page_title="YouTube Data Harvesting",
    page_icon="📊",
    layout="wide",
)

with st.sidebar:
    st.header(':red[YouTube Data Harvesting and Warehousing]')
    
    selected = option_menu(
        menu_title=None,
        options=["Home", "Channel ID Input", "Sample Question"],
        icons=["house", "book", "question-circle"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "#EBEDEF"},
            "icon": {"color": "orange", "font-size": "20px"},
            "nav-link": {"--hover-color": "#ABB2B9", "color": "#FFFFFF", "font-size": "20px"},
            "nav-link-selected": {"background-color": "#2F7E76"},
        }
    )
    
def api_connect():
    api_key = "YOUR-API-KEY"   # Here, put your api-key
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connect()

def Channel_Information(channel_id, youtube):
    request = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails,status'
    )
    try:
        response = request.execute()
    except HttpError as e:
        st.error(f'Error: {e}')
        return None
    
    if 'items' in response:
        item = response['items'][0]
        data = {
            'Channel_Id': item['id'],
            'Channel_Name': item['snippet']['title'],
            'Channel_Description': item['snippet']['description'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Total_Videos': item['statistics']['videoCount'],
            'Total_views': item['statistics']['viewCount'],
            'Channel_Published_date': item['snippet']['publishedAt'],
            'Playlist_Id': item['contentDetails']['relatedPlaylists']['uploads'],
            'Channel_Type': item['status']['privacyStatus']
        }
        return data
    return None

def channel_dataframe(Channel_info):
    df = pd.DataFrame([Channel_info])
    df['Channel_Published_date'] = pd.to_datetime(df['Channel_Published_date'], errors='coerce')
    df['Channel_Published_date'] = df['Channel_Published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def Get_Video_Ids(channel_id):
    request = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    )
    try:
        response = request.execute()
    except HttpError as e:
        st.error(f'Error: {e}')
        return []
    
    if 'items' in response:
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        video_ids = []
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50
        )
        while request:
            try:
                response = request.execute()
                video_ids.extend(item['contentDetails']['videoId'] for item in response.get('items', []))
                request = youtube.playlistItems().list_next(request, response)
            except HttpError as e:
                st.error(f'Error: {e}')
                break
        return video_ids
    return []

def Video_Information(video_ids):
    def batched(iterable, n):
        it = iter(iterable)
        while batch := list(islice(it, n)):
            yield batch
    
    video_details = []
    for batch in batched(video_ids, 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(batch)
        )
        try:
            response = request.execute()
            for item in response.get('items', []):
                video_data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_ID': item['id'],
                    'Video_Title': item['snippet']['title'],
                    'Video_Description': item['snippet']['description'],
                    'Video_Published_date': item['snippet']['publishedAt'],
                    'Video_Views': item['statistics'].get('viewCount', 0),
                    'Video_Likes': item['statistics'].get('likeCount', 0),
                    'Video_Comments': item['statistics'].get('commentCount', 0),
                    'Video_Favorites': item['statistics'].get('favoriteCount', 0),
                    'Video_Duration': item['contentDetails']['duration'],
                    'Thumbnails': item['snippet']['thumbnails']['default']['url'],
                    'Caption_Status': item['contentDetails']['caption']
                }
                video_details.append(video_data)
        except HttpError as e:
            st.error(f'Error: {e}')
            break
    return video_details

def video_dataframe(Video_info):
    df = pd.DataFrame(Video_info)
    df['Video_Published_date'] = pd.to_datetime(df['Video_Published_date'])
    df['Video_Published_date'] = df['Video_Published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['Video_Duration'] = pd.to_timedelta(df['Video_Duration'])
    df['Video_Duration'] = df['Video_Duration'].dt.total_seconds()
    return df

def fetch_comments_for_videos(video_ids, max_comments=100):
    all_comments_data = []
    for video_id in video_ids:
        comments = []
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,
            textFormat='plainText'
        )
        while request and len(comments) < max_comments:
            try:
                response = request.execute()
                if not response.get('items', []):
                    break
                for item in response['items']:
                    comment_info = {
                        'Comment_Id': item['snippet']['topLevelComment']['id'],
                        'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Publish': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comments.append(comment_info)
                    if len(comments) >= max_comments:
                        break
                request = youtube.commentThreads().list_next(request, response)
            except HttpError as e:
                st.error(f'Error: {e}')
                break
        all_comments_data.extend(comments)
    return all_comments_data

def comment_dataframe(comments_data):
    df = pd.DataFrame(comments_data)
    df['Comment_Publish'] = pd.to_datetime(df['Comment_Publish'])
    df['Comment_Publish'] = df['Comment_Publish'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def connection():
    config = {
        'user': 'root',
        'password': 'root',   # Here, given your sql database password
        'host': 'localhost',
        'auth_plugin': 'mysql_native_password',
        'database': 'youtube_data',
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    return conn, cursor

def create_and_insert_channel_table(channel_dataframe):
    conn, cursor = connection()
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_table (
            Channel_Id VARCHAR(100) PRIMARY KEY,
            Channel_Name VARCHAR(100),
            Channel_Description TEXT,
            Subscribers INT,
            Total_Videos INT,
            Total_views INT,
            Channel_Published_date DATETIME,
            Playlist_Id VARCHAR(100),
            Channel_Type VARCHAR(100)
        );
        ''')
        
        insert_query = '''
        INSERT INTO channel_table (
            Channel_Id, Channel_Name, Channel_Description, Subscribers,
            Total_Videos, Total_views, Channel_Published_date, Playlist_Id, Channel_Type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Channel_Name = VALUES(Channel_Name),
            Channel_Description = VALUES(Channel_Description),
            Subscribers = VALUES(Subscribers),
            Total_Videos = VALUES(Total_Videos),
            Total_views = VALUES(Total_views),
            Channel_Published_date = VALUES(Channel_Published_date),
            Playlist_Id = VALUES(Playlist_Id),
            Channel_Type = VALUES(Channel_Type);
        '''
        
        cursor.execute(insert_query, tuple(channel_dataframe.iloc[0]))
        conn.commit()
        st.success("Channel data inserted successfully into the table")
    except Exception as e:
        st.warning(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_and_insert_video_table(video_dataframe):
    conn, cursor = connection()
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS video_table (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(100) PRIMARY KEY,
            Video_Title VARCHAR(100),
            Video_Description TEXT,
            Video_Published_date DATETIME,
            Video_Views INT,
            Video_Likes INT,
            Video_Comments INT,
            Video_Favorites INT,
            Video_Duration FLOAT,
            Thumbnails TEXT,
            Caption_Status VARCHAR(100)
        );
        ''')
        
        insert_query = '''
        INSERT INTO video_table (
            Channel_Name, Channel_Id, Video_Id, Video_Title, Video_Description,
            Video_Published_date, Video_Views, Video_Likes, Video_Comments,
            Video_Favorites, Video_Duration, Thumbnails, Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Channel_Name = VALUES(Channel_Name),
            Channel_Id = VALUES(Channel_Id),
            Video_Title = VALUES(Video_Title),
            Video_Description = VALUES(Video_Description),
            Video_Published_date = VALUES(Video_Published_date),
            Video_Views = VALUES(Video_Views),
            Video_Likes = VALUES(Video_Likes),
            Video_Comments = VALUES(Video_Comments),
            Video_Favorites = VALUES(Video_Favorites),
            Video_Duration = VALUES(Video_Duration),
            Thumbnails = VALUES(Thumbnails),
            Caption_Status = VALUES(Caption_Status);
        '''
        
        for i in range(len(video_dataframe)):
            cursor.execute(insert_query, tuple(video_dataframe.iloc[i]))
        conn.commit()
        st.success("Video data inserted successfully into the table")
    except Exception as e:
        st.warning(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_and_insert_comment_table(comment_dataframe):
    conn, cursor = connection()
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS comment_table (
            Comment_Id VARCHAR(100) PRIMARY KEY,
            Video_Id VARCHAR(100),
            Comment_Text TEXT,
            Comment_Author VARCHAR(100),
            Comment_Publish DATETIME
        );
        ''')
        
        insert_query = '''
        INSERT INTO comment_table (
            Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Publish
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Video_Id = VALUES(Video_Id),
            Comment_Text = VALUES(Comment_Text),
            Comment_Author = VALUES(Comment_Author),
            Comment_Publish = VALUES(Comment_Publish);
        '''
        
        for i in range(len(comment_dataframe)):
            cursor.execute(insert_query, tuple(comment_dataframe.iloc[i]))
        conn.commit()
        st.success("Comment data inserted successfully into the table")
    except Exception as e:
        st.warning(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if selected == "Home":
    st.markdown("<h1 style='text-align: center; color: #184AD8;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
    st.header(":green[Objective]")
    st.write('''The primary objective of this project is to develop a comprehensive YouTube Data Harvesting and Warehousing solution. 
                This involves extracting and transforming YouTube channel and video data, creating a structured relational database to store this information, 
                and implementing an intuitive and interactive Streamlit dashboard for data analysis and querying.''')
    st.header(":green[Technology Used]")
    st.write('''1. **Python**: The primary programming language for data extraction, transformation, and web app development.\n
                2. **Google YouTube API**: Used to access YouTube channel and video data programmatically.\n
                3. **MySQL**: The database system used to store and manage the harvested data.\n
                4. **Streamlit**: A web framework used to build the interactive and user-friendly dashboard for this project.\n
                5. **Pandas**: A Python library used for data manipulation and analysis.\n
                6. **MySQL Connector**: A Python library that facilitates communication between Python and MySQL.''')

if selected == "Channel ID Input":
    st.markdown("<h1 style='text-align: center; color: #184AD8;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
    st.header(':green[Enter a YouTube Channel ID]')
    
    channel_id = st.text_input('Enter Channel ID')
    
    if st.button('Fetch Data') and channel_id:
        channel_info = Channel_Information(channel_id, youtube)
        if channel_info:
            st.subheader('Channel Information')
            st.write(channel_info)
            
            channel_df = channel_dataframe(channel_info)
            st.write(channel_df)
            
            create_and_insert_channel_table(channel_df)
            
            video_ids = Get_Video_Ids(channel_id)
            if video_ids:
                st.subheader('Video Information')
                video_info = Video_Information(video_ids)
                video_df = video_dataframe(video_info)
                st.write(video_df)
                
                create_and_insert_video_table(video_df)
                
                st.subheader('Comment Information')
                comment_info = fetch_comments_for_videos(video_ids)
                if comment_info:
                    comment_df = comment_dataframe(comment_info)
                    st.write(comment_df)
                    
                    create_and_insert_comment_table(comment_df)
                else:
                    st.write("No comments found for the videos.")
            else:
                st.write("No videos found for the channel.")
        else:
            st.write("Invalid Channel ID or API Error")

if selected == "Sample Question":
    st.title(':blue[Sample Question]')
    
    def sql_query_executor(query):
        conn, cursor = connection()
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        conn.commit()
        cursor.close()
        conn.close()
        return df
    
    questions = st.selectbox("Select the Question",
                             ["Q1. What are the names of all the videos and their corresponding channels?",
                              "Q2. Which channels have the most number of videos, and how many videos do they have?",
                              "Q3. What are the top 10 most viewed videos and their respective channels?",
                              "Q4. How many comments were made on each video, and what are their corresponding video names?",
                              "Q5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                              "Q6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?", 
                              "Q7. What is the total number of views for each channel, and what are their corresponding channel names?",
                              "Q8. Which videos have the highest number of comments, and what are their corresponding channel names?",
                              "Q9. Which videos have the highest number of likes and comments combined, and what are their corresponding channel names?",
                              "Q10. Which channels have the most number of views, and how many views do they have?",
                              "Q11. What are the names of all the channels that have published videos in the year 2022?"])
    
    if st.button("Get Solution"):
        if questions == "Q1. What are the names of all the videos and their corresponding channels?":
            df = sql_query_executor("SELECT Channel_Name, Video_Title FROM video_table;")
            st.write(df)
            
        elif questions == "Q2. Which channels have the most number of videos, and how many videos do they have?":
            df = sql_query_executor("SELECT Channel_Name, COUNT(*) AS Total_Videos FROM video_table GROUP BY Channel_Name ORDER BY Total_Videos DESC;")
            st.write(df)
            
        elif questions == "Q3. What are the top 10 most viewed videos and their respective channels?":
            df = sql_query_executor("SELECT Channel_Name, Video_Title, Video_Views FROM video_table ORDER BY Video_Views DESC LIMIT 10;")
            st.write(df)
            
        elif questions == "Q4. How many comments were made on each video, and what are their corresponding video names?":
            df = sql_query_executor("SELECT Video_id, COUNT(*) AS Total_Comments FROM comment_table GROUP BY Video_id ORDER BY Total_Comments DESC;")
            st.write(df)
            
        elif questions == "Q5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            df = sql_query_executor("SELECT Channel_Name, Video_Title, Video_Likes FROM video_table ORDER BY Video_Likes DESC;")
            st.write(df)
            
        elif questions == "Q6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            df = sql_query_executor("SELECT Video_Title, Video_Likes FROM video_table;")
            st.write(df)
            
        elif questions == "Q7. What is the total number of views for each channel, and what are their corresponding channel names?":
            df = sql_query_executor("SELECT Channel_Name, SUM(Video_Views) AS Total_Views FROM video_table GROUP BY Channel_Name ORDER BY Total_Views DESC;")
            st.write(df)
            
        elif questions == "Q8. Which videos have the highest number of comments, and what are their corresponding channel names?":
            df = sql_query_executor("SELECT Channel_Name, Video_Title, Video_Comments FROM video_table ORDER BY Video_Comments DESC;")
            st.write(df)
            
        elif questions == "Q9. Which videos have the highest number of likes and comments combined, and what are their corresponding channel names?":
            df = sql_query_executor("SELECT Channel_Name, Video_Title, (Video_Likes + Video_Comments) AS Total_Interactions FROM video_table ORDER BY Total_Interactions DESC;")
            st.write(df)
            
        elif questions == "Q10. Which channels have the most number of views, and how many views do they have?":
            df = sql_query_executor("SELECT Channel_Name, SUM(Video_Views) AS Total_Views FROM video_table GROUP BY Channel_Name ORDER BY Total_Views DESC;")
            st.write(df)
            
        elif questions == "Q11. What are the names of all the channels that have published videos in the year 2022?":
            df = sql_query_executor("SELECT Channel_Name,Video_Title, Video_Published_date FROM video_table WHERE Video_Published_date >= '2022-01-01' AND Video_Published_date <= '2022-12-31';")
            st.write(df)

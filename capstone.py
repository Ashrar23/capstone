import streamlit as st
import pandas as pd
import pymysql
from googleapiclient.discovery import build
from datetime import datetime

def convert_to_mysql_datetime(iso_datetime):
    try:
        # Parse ISO datetime string into a datetime object
        parsed_datetime = datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%SZ")
        # Convert datetime object to MySQL datetime format string
        mysql_datetime = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")
        return mysql_datetime
    except ValueError:
        st.error("Invalid datetime format provided.")
        return None

# Example usage:
iso_datetime = '2024-03-26T11:39:29Z'
mysql_datetime = convert_to_mysql_datetime(iso_datetime)
print(mysql_datetime)  # Output: '2024-03-26 11:39:29'


def Api_connect():
   ## AIzaSyBXtGe7Y1sg3tWPr01_I637je_S-JmX99g
    Api_id = 'AIzaSyBlnHmI2JVNud0vWXWSZdotlz9lNIRNnWA'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_id)
    return youtube


def create_channels_table(connection):
    cursor = connection.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
                            Channel_Name VARCHAR(255),
                            Channel_Id VARCHAR(255) PRIMARY KEY,
                            Subscribers BIGINT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(255)
                         )'''
        cursor.execute(create_query)
        connection.commit()
        
    except pymysql.Error as e:
        st.error(f"Error creating table: {e}")
    finally:
        cursor.close()


def create_playlists_table(connection):
    cursor = connection.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                            Playlist_Id VARCHAR(255) PRIMARY KEY,
                            Channel_Id VARCHAR(255),
                            Title VARCHAR(255)
                         )'''
        cursor.execute(create_query)
        connection.commit()
        
    except pymysql.Error as e:
        st.error(f"Error creating playlists table: {e}")
    finally:
        cursor.close()

def create_videos_table(connection):
    cursor = connection.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Channel_Name VARCHAR(255),
                            Channel_Id VARCHAR(255),
                            Video_Id VARCHAR(255) PRIMARY KEY,
                            Title VARCHAR(255),
                            Tags TEXT,
                            Thumbnail VARCHAR(255),
                            Description TEXT,
                            Published_Date TIMESTAMP,
                            Duration VARCHAR(255),
                            Views BIGINT,
                            Likes BIGINT,
                            Comments INT,
                            Favorite_Count INT,
                            Definition VARCHAR(255),
                            Caption_Status VARCHAR(255)
                         )'''
        cursor.execute(create_query)
        connection.commit()
        # Remove the success message to prevent it from being displayed every time
    except pymysql.Error as e:
        st.error(f"Error creating videos table: {e}")
    finally:
        cursor.close()  


def create_comments_table(connection):
    cursor = connection.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
                            Comment_Id VARCHAR(255) PRIMARY KEY,
                            Video_Id VARCHAR(255),
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(255),
                            Comment_Published TIMESTAMP
                         )'''
        cursor.execute(create_query)
        connection.commit()
        st.success("Table 'comments' created successfully!")
    except pymysql.Error as e:
        st.error(f"Error creating comments table: {e}")
    finally:
        cursor.close()              


def get_channel_info(channel_id, youtube):
    request = youtube.channels().list(part="snippet,ContentDetails,statistics",
                                      id=channel_id)
    response = request.execute()

    channel_data = []
    for i in response['items']:
        data = dict(channel_Name=i["snippet"]["title"],
                    channel_Id=i["id"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_videos=i['statistics']['videoCount'],
                    channel_desc=i['snippet']['description'],
                    playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
                    )
        channel_data.append(data)

    channel_stored_data = pd.DataFrame(channel_data)
    return channel_stored_data


def get_playlist_details(channel_id, youtube):
    next_page_token = None
    all_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId']
            )
            all_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    playlist_data = pd.DataFrame(all_data)
    return playlist_data


def get_videos_ids(channel_id, youtube):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_info(video_ids, youtube):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

def get_comment_info(video_ids, youtube):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=convert_to_mysql_datetime(item['snippet']['topLevelComment']['snippet']['publishedAt']))

                Comment_data.append(data)

    except Exception as e:
        st.error(f"Error retrieving comments: {e}")

    # Convert Comment_data to DataFrame
    df = pd.DataFrame(Comment_data)
    return df


def insert_into_mysql(channel_data, connection):
    cursor = connection.cursor()
    for index, row in channel_data.iterrows():
        query_check = "SELECT * FROM channels WHERE Channel_Id = %s"
        cursor.execute(query_check, (row['channel_Id'],))
        existing_record = cursor.fetchone()

        if existing_record:
            st.warning(f"Record with Channel_Id {row['channel_Id']} already exists. Skipping insertion.")
            continue

        query_insert = """INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                          VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        values = (row['channel_Name'], row['channel_Id'], row['Subscribers'], row['Views'], row['Total_videos'],
                  row['channel_desc'], row['playlist_Id'])
        cursor.execute(query_insert, values)

    connection.commit()
    cursor.close()


def insert_playlists_into_mysql(playlist_data, connection):
    cursor = connection.cursor()
    try:
        for index, row in playlist_data.iterrows():
            query_check = "SELECT * FROM playlists WHERE Playlist_Id = %s"
            cursor.execute(query_check, (row['Playlist_Id'],))
            existing_record = cursor.fetchone()

            if existing_record:
                st.warning(f"Record with Playlist_Id {row['Playlist_Id']} already exists. Skipping insertion.")
                continue

            query_insert = """INSERT INTO playlists (Playlist_Id, Channel_Id, Title)
                              VALUES (%s, %s, %s)"""
            values = (row['Playlist_Id'][:255], row['Channel_Id'], row['Title'])
            cursor.execute(query_insert, values)

        connection.commit()
        st.success("Playlist data inserted successfully!")

    except pymysql.Error as e:
        st.error(f"Error inserting playlist data: {e}")
    finally:
        cursor.close()
        
def insert_videos_into_mysql(video_data, connection):
    cursor = connection.cursor()
    try:
        for video in video_data:
            # Truncate title if it's too long
            title = video['Title'][:150] if len(video['Title']) > 150 else video['Title']
            # Truncate description if it's too long
            description = video['Description'][:10000] if len(video['Description']) > 10000 else video['Description']
            
            # Convert published date to MySQL datetime format
            published_date_mysql = convert_to_mysql_datetime(video['Published_Date'])

            query_check = "SELECT * FROM videos WHERE Video_Id = %s"
            cursor.execute(query_check, (video['Video_Id'],))
            existing_record = cursor.fetchone()

            if existing_record:
                st.warning(f"Record with Video_Id {video['Video_Id']} already exists. Skipping insertion.")
                continue
            
            # Correct the columns in the query to match the values being inserted
            query_insert = """INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (
                video['Channel_Name'], video['Channel_Id'], video['Video_Id'], title, video['Tags'],
                video['Thumbnail'], description, published_date_mysql, video['Duration'], video['Views'],
                video['Likes'], video['Comments'], video['Favorite_Count'], video['Definition'], video['Caption_Status']
            )
            cursor.execute(query_insert, values)

        connection.commit()
        st.success("Video data inserted successfully!")

    except pymysql.Error as e:
        st.error(f"Error inserting video data: {e}")
    finally:
        cursor.close()

     
def insert_comments_into_mysql(comment_data, connection):
    cursor = connection.cursor()
    try:
        for index, row in comment_data.iterrows():
            query_check = "SELECT * FROM comments WHERE Comment_Id = %s"
            cursor.execute(query_check, (row['Comment_Id'],))
            existing_record = cursor.fetchone()

            if existing_record:
                st.warning(f"Record with Comment_Id {row['Comment_Id']} already exists. Skipping insertion.")
                continue

            query_insert = """INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                              VALUES (%s, %s, %s, %s, %s)"""
            values = (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'],
                      row['Comment_Published'])
            cursor.execute(query_insert, values)

        connection.commit()
        st.success("Comment data inserted successfully!")

    except pymysql.Error as e:
        st.error(f"Error inserting comment data: {e}")
    finally:
        cursor.close()

def main():
    st.title("YouTube Channel Info")

    connection = pymysql.connect(host="localhost",
                                 user="root",
                                 password="Ashrar@23",
                                 database="capstone")
    
    with st.sidebar:
        st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
        st.header("Skill Take Away")
        st.caption("Python Scripting")
        st.caption("Data Collection")
        st.caption("SQL")
        st.caption("API Integration")
        

    
    channel_id = st.text_input("Enter YouTube Channel ID")

    if st.button("Fetch and Store Data"):
        if channel_id:
            youtube = Api_connect()

            channel_data = get_channel_info(channel_id, youtube)
            create_channels_table(connection)
            insert_into_mysql(channel_data, connection)
            st.success("Channel data fetched and stored successfully!")

            playlist_data = get_playlist_details(channel_id, youtube)
            create_playlists_table(connection)
            insert_playlists_into_mysql(playlist_data, connection)
            st.success("Playlist data fetched and stored successfully!")


            video_ids = get_videos_ids(channel_id, youtube)
            video_data = get_video_info(video_ids, youtube)
            create_videos_table(connection)
            insert_videos_into_mysql(video_data, connection)
            st.success("Playlist data fetched and stored successfully!")


            comment_data = get_comment_info(video_ids, youtube)
            create_comments_table(connection)
            insert_comments_into_mysql(comment_data, connection)
            st.success("Comment data fetched and stored successfully!")


        else:
            st.error("Please enter a valid channel ID!")


    # Create a Streamlit dropdown menu for the user to select a question
    question = st.selectbox("Select your question", [
        "All the videos and the channel name",
        "Channels with the most number of videos",
        "10 most viewed videos",
        "Comments in each video",
        "Videos with the highest likes",
        "Likes of all videos",
        "Views of each channel",
        "Videos published in the year of 2022",
        "Average duration of all videos in each channel",
        "Videos with the highest number of comments"
    ])

    # Execute the corresponding SQL query based on the user's selection
    if question == "All the videos and the channel name":
        query = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
    elif question == "Channels with the most number of videos":
        query = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels 
                    ORDER BY total_videos DESC'''
    elif question == "10 most viewed videos":
        query = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos 
                    WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
    elif question == "Comments in each video":
        query = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
    elif question == "Videos with the highest likes":
        query = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount
                    FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
    elif question == "Likes of all videos":
        query = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
    elif question == "Views of each channel":
        query = '''SELECT channel_name AS channelname, views AS totalviews FROM channels'''
    elif question == "Videos published in the year of 2022":
        query = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos
                    WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    elif question == "Average duration of all videos in each channel":
        query = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
    elif question == "Videos with the highest number of comments":
        query = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos 
                    WHERE comments IS NOT NULL ORDER BY comments DESC'''

    # Execute the SQL query
    cursor = connection.cursor()
    cursor.execute(query)

    # Fetch the results and display them in a DataFrame
    results = cursor.fetchall()
    df = pd.DataFrame(results)

    # Show the DataFrame in the Streamlit app
    st.write(df)


def show_channels_details():
    connection = pymysql.connect(host="localhost",
                                 user="root",
                                 password="Ashrar@23",
                                 database="capstone")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM channels")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Channel Name", "Channel ID", "Subscribers", "Views", "Total Videos",
                                     "Channel Description", "Playlist ID"])
    st.dataframe(df)


if __name__ == "__main__":
    main()
    show_channels_details()

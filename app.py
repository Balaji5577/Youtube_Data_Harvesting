#---------------------------------Libraries Used------------------------------------
import streamlit as st
import pymysql
import pymongo
import pandas as pd
import googleapiclient.discovery

#-------------------------------Connecting for MongoDB------------------------------
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['Youtube_Data']
collection = db['Channel_Details']

# ------------------------------MYSQL Connection-----------------------------------
config = {
      'user':'root', 'password':'1234',
      'host':'127.0.0.1', 'database':'youtube_data'
  }
connection = pymysql.connect(**config)
cursor=connection.cursor()

#---------------------------------API Setup-----------------------------------------
api_key="AIzaSyBcNpV8WY47oWtjG3EZ2PzbHGABz97fZEs"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key)


# ------------------------------Getting_Channel_Details------------------------------
def getting_channel_info(channel_id):
  request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id
  )
  response = request.execute()


  for i in response['items']:
    data_to_be_fetched=dict(Channel_name=i["snippet"]["title"],
                        Channel_ID=i["id"],
                        Subscribers=i["statistics"]["subscriberCount"],
                        Views=i["statistics"]["viewCount"],
                        Total_videos=i["statistics"]["videoCount"],
                        Channel_description=i["snippet"]["description"],
                        Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data_to_be_fetched


#-------------------------------Getting_Video_Ids--------------------------------------
def getting_video_ids(channel_id):
  video_Ids=[]
  request1 = youtube.channels().list(id=channel_id,
        part="contentDetails"
    )
  response1 = request1.execute()
  Playlist_Id= response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  request2 = youtube.playlistItems().list(part="snippet",
                                  playlistId=Playlist_Id,maxResults=50)
  response2= request2.execute()

  for i in range(len(response2['items'])):
    video_Ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])

  return video_Ids


#--------------------------------Getting_Video_Information-------------------------------
def getting_video_info(channel_id):
  video_ids=getting_video_ids(channel_id)
  video_data=[]
  for video_id in video_ids:
    request3 = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response3 = request3.execute()

    for item in response3["items"]:
      s = item['contentDetails']['duration']
      l=[]
      f =''
      for i in s:
          if i.isnumeric():
              f = f+i
          else:
              if f:
                  l.append(f)
                  f=''
      if 'H' not in s:
          l.insert(0,'00')
      if 'M' not in s:
          l.insert(1,'00')
      if 'S' not in s:
          l.insert(-1,'00')  
        
      duration  = ':'.join(l)
      result = duration.split(':')
      for i in range(0,3,1):
         if len(result[i]) == 1:
            value = "0" + result[i]
            result.remove(result[i])
            result.insert(i,value)
         final_result = ":".join(result)
            
      data=dict(Channel_Name=item['snippet']['channelTitle'],
            Channel_Id=item['snippet']['channelId'],
            Video_Id=item['id'],
            Video_Title=item['snippet']['title'],
            Video_Description=item['snippet'].get('description'),
            Tags=item['snippet'].get('tags'),
            Published_At=item['snippet']['publishedAt'],
            View_Count=item['statistics'].get('viewCount'),
            Like_Count=item['statistics'].get('likeCount'),
            Comment_count=item['statistics'].get('commentCount'),
            Favourite_count=item['statistics'].get('favoriteCount'),
            Thumbnail=item['snippet']['thumbnails']['default']['url'],
            Caption_Status=item['contentDetails']['caption'],
            Duration=duration
                )
      video_data.append(data)
  return video_data


#-----------------------------------Getting_comment_information------------------------------------
def get_comment_info(channel_id):
  video_ids=getting_video_ids(channel_id)
  Comment_data=[]
  try:
    for video_id in video_ids:
      request4 = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100
        )
      response4 = request4.execute()

      for item in response4["items"]:
        data=dict(Channel_Name=getting_channel_info(channel_id)['Channel_name'],
              Channel_Id=item['snippet']['channelId'],
              Video_Id=item['snippet']['videoId'],
              Comment_Id=item['snippet']['topLevelComment']['id'],
              Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
              Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
              Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
        )
        Comment_data.append(data)
  except:
    pass
  return Comment_data


#-----------------------------------Getting_Playlist_Details-----------------------------------------

def getting_Playlist_details(channel_id):

  Playlist_data=[]

  request5 = youtube.playlists().list(
              part="snippet,contentDetails",
              channelId=channel_id,
              maxResults=50)
  response5 = request5.execute()

  for item in response5["items"]:
          data=dict(Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount'])
          Playlist_data.append(data)

  return Playlist_data

#------------------------------------- Data Storing in MongoDB------------------------------------- 
def youtube_channel_details(channel_id):
    channel_detail=getting_channel_info(channel_id)
    video_IDS=getting_video_ids(channel_id)
    video_detail=getting_video_info(channel_id)
    Comment_detail=get_comment_info(channel_id )
    playlist_detail=getting_Playlist_details(channel_id)

    record = {"Channel_Information":channel_detail,"Video_Id":video_detail,
          "Playlist_Information":playlist_detail,"Comments_Information":Comment_detail}  
    inserted_data = collection.insert_one(record)

    return st.success("Data Stored Successfully")

#-------------------------------------Table Creation for MYSQL---------------------------------------
#----------------------------------------Channel Table-----------------------------------------

def channel_table(channel_id):
  Create_Query = """Create table if not exists channels(Channel_name VARCHAR(100) ,  Channel_ID VARCHAR(50),
                  Subscribers INT, Views INT, Total_videos INT, Channel_description TEXT,
                  Playlist_ID VARCHAR(50), PRIMARY KEY (Channel_ID));"""
  cursor.execute(Create_Query)
  connection.commit()

  d = collection.find_one({'Channel_Information.Channel_ID':channel_id})
  ch_list=d['Channel_Information']
  df=pd.DataFrame(ch_list,index=[0])

  for index,row in df.iterrows():
    insert_query = '''insert into channels(Channel_name,
                                          Channel_ID,
                                          Subscribers,
                                          Views,
                                          Total_videos,
                                          Channel_description,
                                          Playlist_ID)
                                          values(%s,%s,%s,%s,%s,%s,%s)'''
    values=(row['Channel_name'],
            row['Channel_ID'],
            row['Subscribers'],
            row['Views'],
            row['Total_videos'],
            row['Channel_description'],
            row['Playlist_ID'])
    

    cursor.execute(insert_query,values)
    connection.commit()

#----------------------------------------Video Table----------------------------------

def video_table(channel_id):
  Create_Query = """create table if not exists videos(
                Channel_Name varchar(100),
                Channel_ID varchar(100),
                Video_Id varchar(100),
                Video_Title varchar(200),
                Video_Description text,
                Tags text,
                PublishedAt varchar(200),
                View_Count int,
                Like_Count int,
                Favorite_Count int,
                Comment_Count int,
                Duration varchar(50),
                Thumbnail varchar(200),
                Caption_Status varchar(100)
              );"""
  cursor.execute(Create_Query)
  connection.commit()
  
  d = collection.find_one({'Channel_Information.Channel_ID':channel_id})
  vi_list=d['Video_Id']
  df2=pd.DataFrame(vi_list)
  df2.Published_At = df2.Published_At.str.replace('T',',')
  df2.Published_At = df2.Published_At.str.replace('Z','')

  for index,row in df2.iterrows():
    insert_query = '''insert into videos(Channel_Name,
                                        Channel_ID,
                                        Video_Id,
                                        Video_Title,
                                        Video_Description,
                                        Tags,
                                        PublishedAt,
                                        View_Count, 
                                        Like_Count,
                                        Favorite_Count,
                                        Comment_Count,
                                        Duration,
                                        Thumbnail,
                                        Caption_Status)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    row['Tags'] = str(row['Tags'])
    values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Video_Title'],
            row['Video_Description'],
            row['Tags'],
            row['Published_At'],
            row['View_Count'],
            row['Like_Count'],
            row['Favourite_count'],
            row['Comment_count'],
            row['Duration'],
            row['Thumbnail'],
            row['Caption_Status'])
    
    cursor.execute(insert_query,values)
    connection.commit()

#--------------------------------------Playlist Table-------------------------------
    
def playlist_table(channel_id):

  Create_Query = """Create table if not exists playlists(Playlist_Id VARCHAR(100) ,  Title VARCHAR(100),
                  Channel_Id VARCHAR(100), Channel_Name VARCHAR(100), PublishedAt VARCHAR(100), Video_Count INT,
                  PRIMARY KEY (Playlist_Id));"""
  cursor.execute(Create_Query)
  connection.commit()

  d = collection.find_one({'Channel_Information.Channel_ID':channel_id})
  pl_list=d['Playlist_Information']
  df1=pd.DataFrame(pl_list)
  df1.PublishedAt = df1.PublishedAt.str.replace('T',',')
  df1.PublishedAt = df1.PublishedAt.str.replace('Z','')

  for index,row in df1.iterrows():
    insert_query = '''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)
                                            values(%s,%s,%s,%s,%s,%s)'''
    values=(row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['PublishedAt'],
            row['Video_Count'])
    
    cursor.execute(insert_query,values)
    connection.commit()

#----------------------------------------Comment Table---------------------------

def comment_table(channel_id):
  Create_Query = """create table if not exists comments(Channel_Name varchar(100),Channel_ID varchar(100),Video_Id varchar(100),
                                                      Comment_Id varchar(100),Comment_Text text,Comment_Author varchar(100),
                                                      Comment_Published timestamp);"""
  cursor.execute(Create_Query)
  connection.commit()

  d = collection.find_one({'Channel_Information.Channel_ID':channel_id})
  cmt_list=d['Comments_Information']
  df3=pd.DataFrame(cmt_list)
  df3.Comment_Published = df3.Comment_Published.str.replace('T',',')
  df3.Comment_Published = df3.Comment_Published.str.replace('Z','')

  for index,row in df3.iterrows():
    insert_query = '''insert into comments(
                                        Channel_Name,
                                        Channel_ID,
                                        Video_Id,
                                        Comment_Id,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
    values =(
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Comment_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published'])
    
    cursor.execute(insert_query,values)
    connection.commit()

#----------------------------------------Functions for Streamlit---------------------------------
def table_details(channel_id):
    channel_table(channel_id)
    video_table(channel_id)
    playlist_table(channel_id)
    comment_table(channel_id )

    return st.success("Data Successfully Migrated to SQL")

def channel_names():
    channel_list=[]
    for i in collection.find({}):
        channel_list.append(i['Channel_Information']['Channel_name'])
    return channel_list
    
def channel_ids(option):
   id=collection.find_one({'Channel_Information.Channel_name':option})
   id=id['Channel_Information']['Channel_ID']
   return id
#----------------------------------------Streamlit Configuration--------------------------------------
st.set_page_config(page_title="Data Extraction and Migration",page_icon="📁")
st.title(":red[Youtube Data Harvesting and Warehoushing]")
channel_id=st.text_input("Enter the channel id:")
button=st.button("Extract Data")
if button:
   youtube_channel_details(channel_id)


st.write("select a channel to transform data to SQL")
option = st.selectbox('Select Channel',channel_names(),index=None,placeholder='Select Channel')
if st.button('Migrate to SQL'):
        cid = channel_ids(option)
        table_details(cid)

st.title(':red[Data Queries]')   

options = ['1. What are the names of all the videos and their corresponding channels?',
           '2. Which channels have the most number of videos, and how many videos do they have?',
           '3. What are the top 10 most viewed videos and their respective channels?',
           '4. How many comments were made on each video, and what are their corresponding video names?',
           '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
           '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
           '7. What is the total number of views for each channel, and what are their corresponding channel names?',
           '8. What are the names of all the channels that have published videos in the year 2022?',
           '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
           '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
           ]
try:
    select_question = st.selectbox("Select the Question",options,index = None,placeholder='Tap to Select')
    if select_question == '1. What are the names of all the videos and their corresponding channels?':
        query = """select Channel_Name,Video_Title from videos;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Channel Name','Video Title'])
        st.dataframe(df)

    if select_question == '2. Which channels have the most number of videos, and how many videos do they have?':
        query = """select Channel_name,Total_videos from channels order by Total_Videos desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Channel Name','No.of.Videos'])
        st.dataframe(df)

    if select_question == '3. What are the top 10 most viewed videos and their respective channels?':
        query = """select Video_Title, Channel_Name,View_Count from videos order by View_Count desc limit 10;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Video Name','Channel Name','Views'])    
        st.dataframe(df)    

    if select_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        query = """select Video_Title, Comment_Count from videos order by Comment_Count desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Video Name','Comment Count'])    
        st.dataframe(df)

    if select_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query = """select Video_Title, Channel_Name, Like_Count from videos order by Like_Count desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Video Name','Channel Name','Like Count'])    
        st.dataframe(df)    

    if select_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query = """select Video_Title, Like_Count from videos order by Like_Count desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Video Name','Like Count'])    
        st.dataframe(df)     

    if select_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query = """select Channel_name, Views from channels order by Views desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Channel Name','Views'])   
        st.dataframe(df)     

    if select_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        query = """select Channel_Name,Video_Title,date (PublishedAt) from videos where year(PublishedAt)=2022;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Channel Name','Video Name','Published At'])    
        st.dataframe(df)    

    if select_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query = """select Channel_Name, sec_to_time(avg(time_to_sec(Duration))) as Avg_Duration from videos group by Channel_Name order by Avg_Duration ;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Channel Name','Average Duration'])    
        st.dataframe(df)     

    if select_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query = """select Video_Title,Channel_Name,Comment_Count from videos order by Comment_Count desc;"""
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=['Video Name','Channnel Name','Comment Count'])    
        st.dataframe(df)     
except:
    st.success("Please add atleast one channel to MySQL database")

from atproto import Client
import time
#import requests
#import os
import pandas as pd
#import csv

client = Client()
username = ''
password = ''
profile = client.login(username, password)
print('Welcome,', profile.display_name)

params = {
    "q": "wildfire",
    "limit": 25,
    "sort" : 'latest',
    "since": "2025-01-21T00:00:00Z",
    "until": "2025-01-22T00:00:00Z"
}

# READ : https://arxiv.org/pdf/2402.03239

"""
"since": "2025-01-08T00:00:00Z",
"until": "2025-01-09T00:00:00Z"

Gone till 2000 (make cursor equal 2000)

Run this code with cursor = 2000 and update "data_8_01.csv" (can go 80 loop more)

Same Case for
"since": "2025-01-09T00:00:00Z",
"until": "2025-01-10T00:00:00Z"
"""

# CSV File Header : 
# User_DID,User_Handle,Username,Account_Created_At,Followers,Follows,Post_Count,User_Description,Created_At,CID,Text,Image1,Image1_Alt,Image2,Image2_Alt,Image3,Image3_Alt,Image4,Image4_Alt,Likes,Quotes,Reply_Count,Reposts

# API LIKE : https://api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=wildfire&since=2025-01-07T00:00:00Z&until=2025-01-07T23:59:00Z

data_template = {
    "User_DID" : [],
    "User_Handle" : [],
    "Username" : [],
    "Account_Created_At" : [],
    "Followers" : [],
    "Follows" : [],
    "Post_Count" : [],
    "User_Description" : [],
    "Created_At" : [],
    "CID" : [],
    "Text" : [],
    "Image1" : [],
    "Image1_Alt" : [],
    "Image2" : [],
    "Image2_Alt" : [],
    "Image3" : [],
    "Image3_Alt" : [],
    "Image4" : [],
    "Image4_Alt" : [],
    "Likes" : [],
    "Quotes" : [],
    "Reply_Count" : [],
    "Reposts" : [],

}

# Subfolder to save images
#subfolder = "images"
# Ensure the subfolder exists
#os.makedirs(subfolder, exist_ok=True)

#image_num = 1

cursor = None
#cursor = "1550"
last = "0"
entires_fetched = 0
data = data_template.copy()

for loop in range(200):
    if cursor:
        params["cursor"] = cursor
    else:
        cursor = last
        params["cursor"] = cursor
    last = str(int(last) + 25)

    print(f"Loop {loop} started")

    try:
        response = client.app.bsky.feed.search_posts(params)
        print(f"Post Fetched : {len(response.posts)}")
    except:
        print("Posts Not Fetched")
        break

    cursor = response.cursor

    pa = {
        "actors" : [post.author.handle for post in response.posts]
    }

    try:
        pfp = client.app.bsky.actor.get_profiles(pa)
    except:
        print("Profiles Not Fetched")
        break
    
    pfp = pfp.profiles

    if len(pfp) != len(response.posts):
        print(f"Less Number of Profiles Fetched : {len(pfp)}")
        #break

    
    entires_fetched += min(len(pfp), len(response.posts))

    for (i,post) in enumerate(response.posts):
        if i >= len(pfp):
            break
        
        data["User_DID"].append(post.author.did)
        data["User_Handle"].append(post.author.handle)
        data["Username"].append(post.author.display_name)
        data["Account_Created_At"].append(post.author.created_at)
        data["Followers"].append(pfp[i].followers_count)
        data["Follows"].append(pfp[i].follows_count)
        data["Post_Count"].append(pfp[i].posts_count)
        data["User_Description"].append(pfp[i].description)
        data["Created_At"].append(post.record.created_at)
        data["Text"].append(post.record.text)
        data["Likes"].append(post.like_count)
        data["Quotes"].append(post.quote_count)
        data["Reply_Count"].append(post.reply_count)
        data["Reposts"].append(post.repost_count)
        data["CID"].append(post.cid)

        try:
            l = len(post.embed.images)
            for j in range(1,5):
                if (j - 1) >= l:
                    data[f"Image{j}"].append("")
                    data[f"Image{j}_Alt"].append("")
                else:
                    #output_file = os.path.join(subfolder, f"{image_num}.jpeg")
                    data[f"Image{j}"].append(post.embed.images[j - 1].thumb)
                    data[f"Image{j}_Alt"].append(post.embed.images[j - 1].alt)
                    #image_num += 1
                    #re = requests.get(post.embed.images[j - 1].thumb)
                    #if re.status_code == 200:
                        # Write the image content to a file
                        #with open(output_file, "wb") as file:
                            #file.write(re.content)
        except:
            try:
                data["Image2"].append("")
                data["Image2_Alt"].append("")
                data["Image3"].append("")
                data["Image3_Alt"].append("")
                data["Image4"].append("")
                data["Image4_Alt"].append("")
                #re = requests.get(post.embed.thumbnail)
                #output_file = os.path.join(subfolder, f"{image_num}.jpeg")
                data["Image1"].append(post.embed.thumbnail)
                #image_num += 1
                #if re.status_code == 200:
                    # Write the image content to a file
                    #with open(output_file, "wb") as file:
                        #file.write(re.content)
                data["Image1_Alt"].append("")
            except:
                try:
                    xx = post.embed.external
                    #re = requests.get(xx.thumb)
                    data["Image1_Alt"].append(xx.title)
                    #output_file = os.path.join(subfolder, f"{image_num}.jpeg")
                    data["Image1"].append(xx.thumb)
                    #image_num += 1
                    #if re.status_code == 200:
                        # Write the image content to a file
                        #with open(output_file, "wb") as file:
                            #file.write(re.content)
                except:
                    data["Image1"].append("")
                    data["Image1_Alt"].append("")
    response = None
    pfp = None

    """ if entires_fetched > 300:
        
        df = pd.DataFrame(data)
        df.to_csv('dataset3.csv', index=False, mode='a', header=False)
        
        data = data_template.copy()
        print(f"Written {entires_fetched} rows in CSV File")
        entires_fetched = 0
        df = None """


    print(f"Loop {loop} ends with {params["cursor"] if loop > 0 else 0}, {cursor}, {last}")
    print("\n\n")
    time.sleep(5)


df = pd.DataFrame(data)
df.to_csv('data_21_01.csv', index=False, mode='a', header=False)
print(f"Written {entires_fetched} rows in CSV File")
import youtube_dl
import json
from datetime import datetime

lista_finale = []
ydl_opts = {
    'ignoreerrors': True
}




with youtube_dl.YoutubeDL(ydl_opts) as ydl:

        playlist_dict = ydl.extract_info("https://www.youtube.com/user/UCB/videos", download=False)

        for video in playlist_dict['entries']:
            element = {}
            print()

            if not video:
                print('ERROR: Unable to get info. Continuing...')
                continue

            for property in ['upload_date', 'id', 'title', 'duration', 'view_count','like_count', 'dislike_count']:
                print(property, '--', video.get(property))
                if property == 'upload_date':
                    datetimeobject = datetime.strptime(video.get(property),'%Y%m%d')
                    newformatdate = datetimeobject.strftime('%d-%m-%Y')
                    element[property] = str(newformatdate)
                else:
                    element[property]=video.get(property)
            lista_finale.append(element)


text_file = open("video.json", "w")
text_file.write(json.dumps(lista_finale))
text_file.close()
#print(video)
#video_url = video['url']
#print(video_url)
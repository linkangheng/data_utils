from tqdm import tqdm 
import json 
import os 
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Define the global variable
BASE = "Howto-Interlink7M_subset_w_sampled_clips_val"
tsv_path = "/data/hypertext/kangheng/howto100m/Interlink7M_tsv/"+BASE+".tsv"
local_prefix = "/data/hypertext/kangheng/howto100m/samples/"
max_workers = 64
results = []
video_dict = {}

def load_tsv_to_dict(tsv_path):
    """
    return a dict, which format is as folloiw:
    {
        'video':'',
        'clips': {
            '0': {
                'caption':'',
                'duration':''
            },
            '1': {
                'caption':'',
                'duration':''
            }
        }
    }
    """
    df = pd.read_csv(tsv_path, sep="\t")
    video_dict = {}
    for i in tqdm(range(len(df)),total=len(df),desc="loading tsv to dict"):
        clip_dict = {}
        video = df.iloc[i]['video'].split("/")[-1].replace(".mp4", "")
        clips = df.iloc[i]['clips']
        clips = json.loads(str(clips))
        for clip in clips:
            clip_dict[clip["clip_id"]] = {"caption": clip["caption"], "duration": clip["clip"]}
        video_dict[video] = {
            'video': video,
            'clips': clip_dict
        }
    return video_dict


def process_single_video(line):
    '''
    return a json, which format is as follow:
    {
        "video_path": "s3://kanelin/interlink7m/Howto-Interlink7M_subset_w_all_clips_val/--728h8mnDY.mp4",
        "clips": [
            {
                "clip_id": "0",
                "clip_path": "s3://kanelin/interlink7m/Howto-Interlink7M_subset_w_all_clips_val/--728h8mnDY/clip_0.mp4",
                "samples": [frame1, frame2, ...]
                "caption": "caption",
                "duration": "xx:xx - xx:xx"
            },
            {
                "clip_id": "1",
                "clip_path": "s3://kanelin/interlink7m/Howto-Interlink7M_subset_w_all_clips_val/--728h8mnDY/clip_1.mp4",
                "samples": [frame1, frame2, ...]
                "caption": "caption",
                "duration": "xx:xx - xx:xx"
            }
        ]
    }
    ''' 
    jsons=[]
    
    local_video_dir = os.path.join("/data/hypertext/kangheng/howto100m/samples/",BASE,line)
    local_video_path = os.path.join(local_video_dir, line+".mp4")
    video = line
    
    video_path = local_video_path.replace(local_prefix, "s3://kanelin/interlink7m/")
    clip_list = os.listdir(local_video_dir)
    clip_list.sort(key=lambda x: int(x.split("_")[1]))

    for i in clip_list:
        if i.startswith("clip"):
            clip_path = os.path.join(local_video_dir, i)
            clip_remote_path = clip_path.replace(local_prefix, "s3://kanelin/interlink7m/samples/data/")
            clip_id = i.split("_")[-1].split(".")[0]
            samples = []
            for j in os.listdir(clip_path):
                if j.endswith(".jpg"):
                    samples.append(j)
            samples.sort(key=lambda x: int(x.split(".")[0].split("_")[1]))
            
            # 写json
            clip_json = {
                "clip_id": clip_id,
                "clip_path": clip_remote_path,
                "samples": samples,
                "caption": video_dict[video]["clips"][clip_id]["caption"],
                "duration": video_dict[video]["clips"][clip_id]["duration"]
            }
            jsons.append(clip_json)

    # 写入json文件
    result = {
        "video_path": video_path,
        "clips": jsons
    }
    results.append(result)

def combine_jsons(json1, json2):
    # 将两个json文件合并
    # 读取json文件
    json1 = json.loads(json1)
    json2 = json.loads(json2)
    # 合并json文件
    json1.extend(json2)
    return json.dumps(json1)


def main():
    global video_dict    
    video_dict = video_dict = load_tsv_to_dict(tsv_path)
    lines = os.listdir(os.path.join(local_prefix,BASE,""))
    for i in tqdm(lines, total=len(lines), desc="processing videos"):
        process_single_video(i)
    with open("test.json", "w") as f:
        f.write(json.dumps(results, indent=4))
        f.close()

def multithreading_main():
    global video_dict    
    video_dict = video_dict = load_tsv_to_dict(tsv_path)
    print("Finish loading tsv to dict!\n")
    lines = os.listdir(os.path.join(local_prefix,BASE,""))
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_single_video, line) for line in lines]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing videos"):
            future.result()
    with open(BASE+".json", "w") as f:
        f.write(json.dumps(results, indent=4))
        f.close()

if __name__ == "__main__":
    # main()
    multithreading_main()

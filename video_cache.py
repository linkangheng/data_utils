import cv2
import os
import shutil
import re
from megfile import smart_open, smart_exists, smart_sync, smart_remove, smart_glob
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import shutil
import threading
import math
from tqdm import tqdm 
import numpy as np
import argparse

# Define global variables
TYPE = "Howto-Interlink7M_subset_w_all_clips_train" #Howto-Interlink7M_subset_w_all_clips_train,Howto-Interlink7M_subset_w_all_clips_val,Howto-Interlink7M_subset_w_sampled_clips_train，Howto-Interlink7M_subset_w_sampled_clips_val
max_workers = 512
DELETE_THRESHOLD = 1000
delete_counter = 0
progress_count = 0
total_videos_count = 1847380   # all-train:1847380 all-val:237730 sampled-train:3679476 sampled-val:315160
cached_files = []

# Define log path for tos
error_log_path = "/data/hypertext/kangheng/project/data_utils/"+TYPE+"_"+"error_details.txt"
error_list_path = "/data/hypertext/kangheng/project/data_utils/"+TYPE+"_"+"error_list.txt"
progress_log_path = "/data/hypertext/kangheng/project/data_utils/"+TYPE+"_"+"progress.txt"  #For Howto-Interlink7M_subset_w_all_clips_val, the TYPE is not need.

def log(message,log_path):
    with open(log_path, 'a') as log_file:
        log_file.write(message + '\n')

def get_cache_video(video_path):
    # Determine if the video exists
    if not smart_exists(video_path):
        error_message = f"Video file not found: {video_path}"
        log(error_message,error_log_path)
        log(video_path,error_list_path)
        return 
    
    # Caching the video
    with smart_open(video_path, 'rb') as file_obj:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
            shutil.copyfileobj(file_obj, temp_file)
            cache_video_path = temp_file.name
    return cache_video_path

def get_devided_lines(lines,machine_id,interval=8):
    # TODO Divide the lines into 8 parts and run them on different machines
    # 划分数组lines，并取第machine_id个部分
    lines = lines[int(len(lines)/interval)*machine_id:int(len(lines)/interval)*(machine_id+1)]
    lines = resume_lines(lines)
    return lines

def resume_lines(lines):
    prefix = "s3://kanelin/interlink7m/"+TYPE+"/"
    print("Restarting from last processing endpoint...\n")
    processed = []
    try:
        with open(progress_log_path, 'r') as log_file:
            for i in log_file:
                processed.append(prefix+re.search(r'\((.*?)\)', i).group(1))
            processed = set(processed)
    except FileNotFoundError:
        processed=[]
    lines = [line for line in lines if line not in processed]
    print("Load all unprocessed videos!")
    return lines

def sample_last_frame_from_oringle(output_folder):
    # This function is used to handle situations where the last video segment is too short to load
    original_video = output_folder.replace("/data/hypertext/kangheng/howto100m/samples","s3://kanelin/interlink7m") # for brainpp, original_video = output_folder.replace("/data/howto100m/samples","s3://kanelin/interlink7m")
    original_video = "/".join(original_video.split("/")[:-1])+"/"+original_video.split("/")[-2]+".mp4"
    original_video = get_cache_video(original_video)
    cap = cv2.VideoCapture(original_video)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.set(cv2.CAP_PROP_POS_FRAMES, total_frames - 1)
    ret, frame = cap.read()
    if ret:
        output_path = os.path.join(output_folder, "frame_0.jpg")
        cv2.imwrite(output_path, frame)
        os.remove(original_video)
    else:
        assert "Error: Happens when sample last frame from oringle video"
    
def extract_frames(video_path, output_folder):
    interval = 13
    
    os.makedirs(output_folder, exist_ok=True)
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    
    if total_frames < (interval-1) * fps:
        frame_step = fps
    else:
        frame_step = (total_frames - 1) // (interval -2)

    frame_count = 0
    sample_num = 0
    success = True

    while success:
        success, image = cap.read()
        if not success:
            # Unable to read video normally,so use the last frame of the original video as sample
            sample_last_frame_from_oringle(output_folder)
            return   
        if (frame_count % frame_step == 0 and frame_count != total_frames) or frame_count == 0 or frame_count == total_frames - 1:
            try:
                if sample_num > interval:
                    assert False, "sample_num over interval!"
                cv2.imwrite(f'{output_folder}/frame_{frame_count}.jpg', image)
                sample_num += 1
            except:
                import ipdb
                ipdb.set_trace()
        frame_count += 1

    cap.release()


def get_processed(log_file):
    try:
        with open(log_file,"r") as log:
            return len(log.readlines())
    except FileNotFoundError:
        return 0

def load_lines(lines_txt,prefix=""):
    if len(prefix)!=0:
        lines_txt = prefix + lines_txt
    lines = []
    with open(lines_txt,'r') as file:
        for line in file:
            lines.append(line.strip())
    return lines 

def sample_process(local_video_path,s3_video_path):
    output_folder = s3_video_path.replace("s3://kanelin/interlink7m", "/data/hypertext/kangheng/howto100m/samples")[:-4] # for brainpp, output_folder = s3_video_path.replace("s3://kanelin/interlink7m", "/data/howto100m/samples")[:-4]
    extract_frames(local_video_path,output_folder=output_folder)

def tracking_process(local_video_path):
    # TODO track all the videos 
    pass


def process_video(video_path):
    global delete_counter
    global cached_files
    global progress_count
    
    cache_video_path = get_cache_video(video_path)
    
    # Process the video
    try:
        sample_process(cache_video_path,video_path)
        with threading.Lock():
            progress_count += 1
            video = "/".join(video_path.split("/")[-2:])
            progress_message = f"Processed video {progress_count}/{total_videos_count} ({video})"
            log(progress_message,progress_log_path)
            
    except Exception as e:
        error_message = f"Error processing file {video_path}: {e}"
        print(error_message)

    # Delete the cache
    cached_files.append(cache_video_path)
    delete_counter += 1
    
    if delete_counter >= DELETE_THRESHOLD:
        for cached_file in cached_files:
            os.remove(cached_file)
        delete_counter = 0
        cached_files = []
    
    
def main():
    global progress_count
    # Get the number of processed video
    progress_count = get_processed(progress_log_path)
    
    lines_file = "/data/hypertext/kangheng/project/merlin_track/videos/txt/" + TYPE + ".txt"
    lines = load_lines(lines_file) 
    lines = resume_lines(lines)

    # for line in tqdm(lines,total=len(lines)):
    #     process_video(line)
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_video, line) for line in lines]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing videos"):
            future.result()

def ditribute_main(machine_id):
    global progress_count
    global error_log_path
    global error_list_path
    global progress_log_path
    
    # Maintain logs based on machine ID for TOS
    log_prefix=os.path.join("/data/hypertext/kangheng/project/data_utils/result",TYPE) # for brainpp, log_prefix=os.path.join("/data/result",TYPE) 
    error_log_path = os.path.join(log_prefix,str(machine_id)+"_error_details.txt")
    error_list_path = os.path.join(log_prefix,str(machine_id)+"_error_list.txt")
    progress_log_path = os.path.join(log_prefix,str(machine_id)+"_progress.txt")
    
    # Get the number of processed video
    progress_count = get_processed(progress_log_path)
    lines_file = "/data/hypertext/kangheng/project/merlin_track/videos/txt/" + TYPE + ".txt" # for brainpp, lines_file = "/data/videos/txt/" + TYPE + ".txt"
    lines = load_lines(lines_file)
    lines = get_devided_lines(lines,machine_id,interval=10)
    
    # for line in tqdm(lines,total=len(lines)):
    #     process_video(line)
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_video, line) for line in lines]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing videos"):
            future.result()

def debug():
    line = "s3://kanelin/interlink7m/Howto-Interlink7M_subset_w_all_clips_train/26n5ePOXc5I/clip_3.mp4"
    process_video(line)

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--machine_id", type=int, default=0,help="Machine ID")
    # args = parser.parse_args()
    # machine_id = args.machine_id
    machine_id = 0
    ditribute_main(machine_id)
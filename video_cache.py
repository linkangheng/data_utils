import cv2
from megfile import smart_open, smart_exists, smart_sync, smart_remove, smart_glob
import tempfile
import shutil
import threading
import math

# Define global variables
DELETE_THRESHOLD = 5
delete_counter = 0
progress_count = 0
total_videos_count = 1000 # to count
cached_files = []

# Define log path
error_log_path = "xxx"
error_list_path = "xxx"
progress_log_path = "xxx"

def log(message,log_path):
    with open(log_path, 'a') as log_file:
        log_file.write(message + '\n')

def extract_frames(video_path, output_folder):
    interval = 13
    
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
        if frame_count % frame_step == 0 or frame_count == 0 or frame_count == total_frames - 1:
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
    with open(log_file,"r") as log:
        return len(log.readlines())

def load_lines(lines_txt):
    lines = []
    with open(lines_txt,'r') as file:
        for line in file:
            lines.append(line.strip())
    return lines 

def process(video_path):
    pass

def process_video(video_path):
    global delete_counter
    global cached_files
    global progress_count
    
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
    
    # Process the video
    try:
        process(cache_video_path)
        with threading.Lock():
            progress_count += 1
            progress_message = f"Processed video {progress_count}/{total_videos_count} ({video_path})"
            log_progress(progress_message,progress_log_path)
        
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
    lines = load_lines(txt_file) # 这里想先把已有的所有clip_path 统计成一个txt，再进行统一处理
    
    process_video(line)
    
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_video, line) for line in lines]
        for future in concurrent.futures.as_completed(futures):
            future.result()  

def debug():
    line = "s3://kanelin/interlink7m/Howto-Interlink7M_subset_w_all_clips_train/26n5ePOXc5I/clip_2.mp4"
    process_video(line)

    
debug()
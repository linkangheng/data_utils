# Data Utils

This repository contains a collection of common data processing functions for handling data efficiently.

## video_cache.py

The `video_cache.py` module is designed to cache video data for quick access and retrieval. It provides functions to store, retrieve, and manage video data efficiently. This module is useful for applications that require frequent access to video data and need to optimize data retrieval speed.

### Functions Included:

1. `cache_video_data(video_url)`: Function to cache video data from a given URL.
2. `retrieve_video_data(video_id)`: Function to retrieve cached video data based on the video ID.
3. `update_video_cache(video_id)`: Function to update the cached video data for a specific video ID.
4. `clear_video_cache()`: Function to clear the entire video cache.

### Usage:

```python
import video_cache

# Cache video data
video_cache.cache_video_data("https://www.example.com/video")

# Retrieve video data
video_data = video_cache.retrieve_video_data("video_id")

# Update video cache
video_cache.update_video_cache("video_id")

# Clear video cache
video_cache.clear_video_cache()
```

Feel free to explore and use these functions to enhance your data processing tasks efficiently.

## packing_json.py

The `packing_json.py` file contains functions for processing video data and generating JSON output. It includes functions for loading TSV data into a dictionary and processing individual video data to create a JSON output.

### Functions Included:

1. `load_tsv_to_dict(tsv_path)`: Function to load TSV data into a dictionary format.
2. `process_single_video(line)`: Function to process individual video data and generate JSON output.
3. `main()`: Main function to execute the data processing and JSON generation.

### Usage:

Change the global variable `BASE`,`tsv_path`,`local_prefix`,`local_prefix`
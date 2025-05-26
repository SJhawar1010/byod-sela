from google.cloud import storage, speech
from moviepy.editor import VideoFileClip
from pydub import AudioSegment
import os
import json
from fpdf import FPDF
import logging

from flask import Flask, request, jsonify

import os
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

@app.route('/')
def index():
    return 'MoviePy Cloud Run is working!'

@app.route('/convert')
def covnvert_video():
    
    storage_client = storage.Client()
    speech_client = speech.SpeechClient()

    # input_bucket_name = "videodatasetselaseva"
    # input_video_name = "Responsible AI.mp4"
    # output_audio_name = "Responsible AI.flac"
    # output_bucket_name = "audiodatasetselaseva"
    # output_folder_name = "sevadataset/"
    # pdf_bucket_name = "pdfdatasetselaseva"
    input_bucket_name = os.environ["INPUT_BUCKET_NAME"]
    input_video_name = os.environ["INPUT_VIDEO_NAME"]
    output_audio_name = os.environ["OUTPUT_AUDIO_NAME"]
    output_bucket_name = os.environ["OUTPUT_BUCKET_NAME"]
    output_folder_name = os.environ["OUTPUT_FOLDER_NAME"]
    pdf_bucket_name = os.environ["PDF_BUCKET_NAME"]

    video_bucket = storage_client.bucket(input_bucket_name)
    video_blob = video_bucket.blob(input_video_name)
    video_blob.download_to_filename(input_video_name)

    video = VideoFileClip(input_video_name)
    video.audio.write_audiofile("temp_audio.wav")

    audio = AudioSegment.from_wav("temp_audio.wav")
    audio.export(output_audio_name, format="flac")

    audio_bucket = storage_client.bucket(output_bucket_name)
    audio_blob = audio_bucket.blob(output_audio_name)
    audio_blob.upload_from_filename(output_audio_name)

    audio_uri = f"gs://{output_bucket_name}/{output_audio_name}"
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=44100,
        language_code="en-US",
        audio_channel_count=2,  # Ensure this matches the FLAC file's channels
        enable_word_time_offsets=True
    )

    audio = speech.RecognitionAudio(uri=audio_uri)
    operation = speech_client.long_running_recognize(config=config, audio=audio)
    response = operation.result(timeout=1000)  # Adjust timeout as needed

    for result in response.results:
        logging.info("Transcription:", result.alternatives[0].transcript)

    gcs_video_url = "https://storage.googleapis.com/videodatasetselaseva/Responsible%20AI.mp4"

    # Store metadata
    metadata = {
        "gcs_video_url": gcs_video_url
    }

    bucket = storage_client.lookup_bucket(pdf_bucket_name)
    # Save metadata as a JSON file in Cloud Storage
    metadata_blob = bucket.blob("dataset/transcription_metadata.json")
    metadata_blob.upload_from_string(json.dumps(metadata), content_type="application/json")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    for result in response.results:
        for word_info in result.alternatives[0].words:
            word = word_info.word
            start_time = int(word_info.start_time.total_seconds())  # Convert to seconds
            gcs_link = f"{gcs_video_url}#t={start_time}"  # Append timestamp
            pdf.cell(0, 10, f"{word} [{start_time}s] - {gcs_link}", ln=True)

    input_video_name
    FILE_NAME = os.path.splitext(input_video_name)[0] + '.pdf'
    pdf.output(FILE_NAME)
    logging.info(f'Transcription with timestamps saved as {FILE_NAME}')


    blob = bucket.blob(f"{output_folder_name}/{FILE_NAME}")
    blob.upload_from_filename(FILE_NAME)
    loggin.info(f'PDF uploaded to GCP bucket {pdf_bucket_name} in folder {output_folder_name}.')
    return jsonify({"message": "PDF uploaded to GCP bucket {pdf_bucket_name} in folder {output_folder_name}", "code": 200})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

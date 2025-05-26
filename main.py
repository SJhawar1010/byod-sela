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

    # input_bucket_name = os.environ["INPUT_BUCKET_NAME"]
    # output_bucket_name = os.environ["OUTPUT_BUCKET_NAME"]
    # output_folder_name = os.environ["OUTPUT_FOLDER_NAME"]
    # pdf_bucket_name = os.environ["PDF_BUCKET_NAME"]

    input_bucket_name = os.environ["INPUT_BUCKET_NAME"]
    output_audio_bucket_name = os.environ["OUTPUT_BUCKET_NAME"]
    output_pdf_bucket_name = os.environ["PDF_BUCKET_NAME"]
    pdf_folder_name = os.environ["OUTPUT_FOLDER_NAME"]
    metadata_folder_name = os.environ["OUTPUT_MFOLDER_NAME"]

    video_bucket = storage_client.bucket(input_bucket_name)
    video_blobs = list(video_bucket.list_blobs())

    for blob in video_blobs:
        if not blob.name.lower().endswith(".mp4"):
            continue  # Skip non-video files

        input_video_name = blob.name
        local_video_path = input_video_name.replace("/", "_")  # Avoid nested paths
        logging.info(f"\nProcessing video: {input_video_name}")

        # Download the video file
        blob.download_to_filename(local_video_path)

        # Extract audio
        video = VideoFileClip(local_video_path)
        temp_audio_path = "temp_audio.wav"
        video.audio.write_audiofile(temp_audio_path)

        # Convert to FLAC
        output_audio_name = input_video_name.rsplit(".", 1)[0] + ".flac"
        audio = AudioSegment.from_wav(temp_audio_path)
        audio.export(output_audio_name, format="flac")

        # Upload audio to GCS
        audio_bucket = storage_client.bucket(output_audio_bucket_name)
        audio_blob = audio_bucket.blob(output_audio_name)
        audio_blob.upload_from_filename(output_audio_name)

        # Prepare audio URI for transcription
        audio_uri = f"gs://{output_audio_bucket_name}/{output_audio_name}"

        # Configure transcription
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
            sample_rate_hertz=44100,
            language_code="en-US",
            audio_channel_count=2,
            enable_word_time_offsets=True
        )

        audio_request = speech.RecognitionAudio(uri=audio_uri)

        logging.info(f"Starting transcription...{input_video_name}")
        operation = speech_client.long_running_recognize(config=config, audio=audio_request)
        response = operation.result(timeout=1000)

        # Prepare metadata
        gcs_video_url = f"https://storage.googleapis.com/{input_bucket_name}/{input_video_name.replace(' ', '%20')}"
        metadata = {"gcs_video_url": gcs_video_url}
        metadata_blob = storage_client.bucket(output_pdf_bucket_name).blob(f"{metadata_folder_name}{input_video_name.rsplit('.', 1)[0]}_metadata.json")
        metadata_blob.upload_from_string(json.dumps(metadata), content_type="application/json")

        # Generate PDF with timestamps
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        for result in response.results:
            for word_info in result.alternatives[0].words:
                word = word_info.word
                start_time = int(word_info.start_time.total_seconds())
                gcs_link = f"{gcs_video_url}#t={start_time}"
                pdf.cell(0, 10, f"{word} [{start_time}s] - {gcs_link}", ln=True)

        pdf_file_name = input_video_name.rsplit(".", 1)[0].replace("/", "_") + ".pdf"
        pdf.output(pdf_file_name)

        # Upload PDF to GCS
        pdf_bucket = storage_client.bucket(output_pdf_bucket_name)
        pdf_blob = pdf_bucket.blob(f"{pdf_folder_name}{pdf_file_name}")
        pdf_blob.upload_from_filename(pdf_file_name)

        logging.info(f"✅ Done: {input_video_name} ➜ {pdf_file_name}") 
        return jsonify({"message": "All PDF uploaded to GCP bucket", "code": 200})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

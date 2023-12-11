import subprocess

# List of script files in the order you want to run them
script_files = [
    "1_api_saved_locally.py",
    "2_transfer_api_output_to_hdfs.py",
    "3_extract_top_1_word_for_day.py",
    "4_filter_articles_by_top_word_and_sentiment.py",
    "5_trasfer_acticles_to_elstic.py",
    "6_monitoring.py",
    "elasticsearch_client.py",
    "exc_elastic.py",
    "kafka_to_elasticsearch.py",
    "save_tweets_to_hdfs.py",
    "twitter_producer.py"
]

def run_scripts():
    for script_file in script_files:
        print(f"Running script: {script_file}")
        try:
            subprocess.run(["python", script_file], check=True)
            print(f"Script {script_file} executed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error executing script {script_file}: {e}")
            break

if __name__ == "__main__":
    run_scripts()

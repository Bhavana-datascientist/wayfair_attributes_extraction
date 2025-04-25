from flask import Flask, request, jsonify
import os, json, base64, io, requests, threading, time
from PIL import Image
from openai import OpenAI
from pymongo import MongoClient
from queue import Queue

app = Flask(__name__)


client = OpenAI(
    api_key='s-proj-omCLH6a6JnhvVQfRcxBBnhIUTF04gTovOPzVTRR_zvz9xG2hq9toocWfPnBBhX1K591Od5S775T3BlbkFJGvQrO3fZKKJfNkp_8iV_aLQFKc9LEsax49dssaJT9_EHS6EqociSGl47eOEzancOAbFqJXzJIA')
mongo_client = MongoClient(
    "mongodb+srv://bhavana:Trends_bhavana@wayfair.xve1u.mongodb.net/?retryWrites=true&w=majority&tls=true&tlsAllowInvalidCertificates=true")

DOWNLOAD_FOLDER = 'D:/Furniture_trends/download_images'
JSONL_OUTPUT = 'D:/Furniture_trends/batch_input_thread.jsonl'
OUTPUT_PATH = 'D:/Furniture_trends/batch_output_thread.jsonl'
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

CATEGORY_MAPPINGS = {
    "Sofa": {"db": "console", "data_collection": "wayfair_sofa", "config_collection": "sofa_taxonomy"},
    "Coffee Table": {"db": "console", "data_collection": "wayfair_coffee_table",
                     "config_collection": "coffee table taxonomy"},
    "Accent Chair": {"db": "console", "data_collection": "wayfair_accent_chair",
                     "config_collection": "accent chair taxonomy"}
}

batch_queue = Queue()


def resize_image(image_path, max_size=512):
    with Image.open(image_path) as img:
        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
        img.save(image_path)
    return image_path


def encode_image_to_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')


def get_config_for_category(category):
    mapping = CATEGORY_MAPPINGS[category]
    config = mongo_client[mapping["db"]][mapping["config_collection"]].find_one()
    if config:
        config.pop("_id", None)
        return config
    raise ValueError(f"No config found for category {category}")


def generate_batch_input(category):
    mapping = CATEGORY_MAPPINGS[category]
    config = get_config_for_category(category)
    products = list(mongo_client[mapping["db"]][mapping["data_collection"]].find().limit(3))

    with open(JSONL_OUTPUT, 'w', encoding='utf-8') as jsonl_file:
        for idx, row in enumerate(products):
            image_url = row.get('Image_URL')
            description = str(row.get('Concatenated_information', ""))
            if not image_url:
                continue
            try:
                resp = requests.get(image_url, timeout=10)
                resp.raise_for_status()
                img = Image.open(io.BytesIO(resp.content))
                img_format = img.format.lower() or 'jpg'
                local_path = os.path.join(DOWNLOAD_FOLDER, f'image_{idx}.{img_format}')
                img.save(local_path)
                resized_path = resize_image(local_path)
                base64_image = encode_image_to_base64(resized_path)
                attr_prompts = [f"{k}: [{', '.join(v)}]" for k, v in config.items()]
                prompt = f"""
You are an advanced AI trained to extract structured data from furniture images.
Analyze the image and output the following attributes **strictly in JSON format only** using only values from the list provided.
Attributes to extract:{', '.join(config.keys())}
Use only these allowed values:
{'; '.join(attr_prompts)}
Description: {description}
Return only the JSON. Do not include explanations, markdown formatting, or any text outside the JSON.
"""
                req = {
                    "custom_id": f"request-{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-4o",
                        "messages": [
                            {"role": "user", "content": [
                                {"type": "text", "text": prompt},
                                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                            ]}
                        ],
                        "max_tokens": 700
                    }
                }
                jsonl_file.write(json.dumps(req) + '\n')
            except Exception as e:
                print(f"❌ Error on row {idx}: {e}")
    return prompt


def run_batch_and_save(prompt_text):
    upload = client.files.create(file=open(JSONL_OUTPUT, "rb"), purpose="batch")
    batch = client.batches.create(input_file_id=upload.id, endpoint="/v1/chat/completions", completion_window="24h")

    # Save to Batch_table
    batch_meta = {
        "batch_id": batch.id,
        "model": "gpt-4o",
        "prompt": prompt_text,
        "status": "submitted",
        "created_at": time.time()
    }
    mongo_client["console"]["Batch_table"].insert_one(batch_meta)

    return batch.id


def download_output_and_store(batch_id, category):
    batch = client.batches.retrieve(batch_id)
    if batch.status != "completed":
        return False

    output_file_id = batch.output_file_id
    if not output_file_id:
        return False

    response = client.files.content(output_file_id)
    with open(OUTPUT_PATH, "wb") as f:
        f.write(response.read())

    collection = mongo_client["console"]["Attributes_Extraction_thread"]
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                content = data["response"]["body"]["choices"][0]["message"]["content"]
                parsed = json.loads(content.strip("`").replace("json", "").strip())
                parsed["request_id"] = data.get("custom_id")
                parsed["category"] = category
                parsed["batch_id"] = batch.id
                print(f"Inserting document with batch_id: {batch_id}")
                collection.insert_one(parsed)
            except Exception as e:
                print(f"⚠️ Line {i}: Error - {e}")
    return True


def batch_monitor():
    while True:
        time.sleep(60)
        if not batch_queue.empty():
            info = batch_queue.queue[0]
            batch_id, category = info["batch_id"], info["category"]
            status = client.batches.retrieve(batch_id)
            if status.status == "completed":
                print(f"✅ Batch {batch_id} is complete! Parsing and saving...")
                download_output_and_store(batch_id, category)
                mongo_client["console"]["Batch_table"].update_one({"batch_id": batch_id},
                                                                  {"$set": {"status": "completed"}})
                batch_queue.get()


@app.route('/extract_attributes', methods=['POST'])
def extract_attributes():
    try:
        category = request.json.get("category")
        prompt_text = generate_batch_input(category)
        batch_id = run_batch_and_save(prompt_text)
        batch_queue.put({"batch_id": batch_id, "category": category})
        return jsonify({"message": "Batch job submitted", "batch_id": batch_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/check_batch_status', methods=['POST'])
def check_batch_status():
    try:
        batch_id = request.json.get("batch_id")
        batch = client.batches.retrieve(batch_id)
        return jsonify({
            "status": batch.status,
            "completed_at": batch.completed_at,
            "output_file_id": batch.output_file_id
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


threading.Thread(target=batch_monitor, daemon=True).start()

if __name__ == '__main__':
    app.run(port=5000, debug=True)

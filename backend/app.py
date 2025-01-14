from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import requests
import os
from langchain_openai import AzureChatOpenAI
from helper_functions import get_rfp_analysis_from_db
from enhance import enhance_resume
from upload import process_rfp
from search import search
import mammoth
import io
from pydocx import PyDocX

load_dotenv()

app = Flask(__name__)
CORS(app)


connect_str = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
container_name = os.getenv("STORAGE_ACCOUNT_CONTAINER")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")

# Environment variables
COSMOS_HOST = os.getenv('COSMOS_HOST')
COSMOS_MASTER_KEY = os.getenv('COSMOS_MASTER_KEY')
COSMOS_DATABASE_ID = os.getenv('COSMOS_DATABASE_ID')
COSMOS_CONTAINER_ID = os.getenv('COSMOS_CONTAINER_ID')

AOAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AOAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AOAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")

STORAGE_ACCOUNT_CONNECTION_STRING = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
STORAGE_ACCOUNT_CONTAINER = os.getenv("STORAGE_ACCOUNT_CONTAINER")
STORAGE_ACCOUNT_RESUME_CONTAINER = os.getenv("STORAGE_ACCOUNT_RESUME_CONTAINER")

# Initialize Azure services
primary_llm = AzureChatOpenAI(
    azure_deployment=AOAI_DEPLOYMENT,
    api_version="2024-05-01-preview",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=AOAI_KEY,
    azure_endpoint=AOAI_ENDPOINT
)

cosmos_client = CosmosClient(COSMOS_HOST, {'masterKey': COSMOS_MASTER_KEY})
database = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
container = database.get_container_client(COSMOS_CONTAINER_ID)

blob_service_client = BlobServiceClient.from_connection_string(STORAGE_ACCOUNT_CONNECTION_STRING)
blob_container_client = blob_service_client.get_container_client(STORAGE_ACCOUNT_CONTAINER)
blob_resume_container_client = blob_service_client.get_container_client(STORAGE_ACCOUNT_RESUME_CONTAINER)

def get_rfps_from_blob_storage():
    rfps = []
    blobs = blob_container_client.list_blobs()
    for blob in blobs:
        rfps.append({"name": blob.name, "status": "Complete"})
    return rfps

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    try:
        file_content = file.read()
        return process_rfp(file_content, file.filename)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/available-rfps', methods=['GET'])
def get_rfps():
    blob_rfps = get_rfps_from_blob_storage()
    return jsonify(blob_rfps)

@app.route('/get-rfp-analysis', methods=['GET'])
def get_rfp_analysis():
    rfp_name = request.args.get('rfp_name')
    result = get_rfp_analysis_from_db(rfp_name)
    
    if result == "RFP name is required":
        return jsonify({"error": result}), 400
    elif result == "RFP analysis not found":
        return jsonify({"error": result}), 404
    elif result.startswith("An error occurred"):
        return jsonify({"error": result}), 500
    else:
        return jsonify({"skills_and_experience": result}), 200

@app.route('/search', methods=['POST'])
def search_employees():
    data = request.json
    rfp_name = data.get('rfpName')
    feedback = data.get('feedback')

    if not rfp_name:
        return jsonify({"error": "RFP name is required"}), 400

    try:
        results = search(rfp_name, feedback)
        return jsonify({"results": results}), 200
    except Exception as e:
        print(f"Error during search: {str(e)}")
        return jsonify({"error": "An error occurred during the search"}), 500

def generate_mock_enhanced_resume_link(resume_id, rfp_name):
    # This is a mock function to generate a fake enhanced resume link
    return f"http://example.com/enhanced-resumes/{resume_id}.pdf"

@app.route('/enhance', methods=['POST'])
def enhance():
    data = request.json
    resume_name = data.get('resumeName')
    rfp_name = data.get('rfpName')
    search_query = data.get('search_query')
    
    if not resume_name or not rfp_name:
        return jsonify({"error": "Missing resumeName or rfpName"}), 400

    try:
        # Call the enhance_resume function from enhance.py
        
        enhanced_resume_name = enhance_resume(resume_name, rfp_name, search_query)
        
        # Generate the URL for the enhanced resume
        enhanced_resume_url = f"{request.host_url}download?resumeName={enhanced_resume_name}"
        
        return jsonify({
            "enhancedResumeLink": enhanced_resume_url,
            "enhancedResumeName": enhanced_resume_name
        }), 200
    except Exception as e:
        print(f"Error enhancing resume: {str(e)}")
        return jsonify({"error": "An error occurred while enhancing the resume"}), 500

# @app.route('/resume', methods=['GET'])
# def get_resume():
#     resume_name = request.args.get('resumeName')
#     print(f"Input: {resume_name}")
    
    # if resume_name.startswith('Tailored/'):
    #     # For enhanced resumes, we already have the full blob name
    #     blob_name = resume_name.replace('.docx', '.pdf')
        
    # else:
    #     # For regular resumes, construct the blob name
    #     blob_name = resume_name.replace('.docx', '.pdf')
    #     blob_name = f'pdf/{blob_name}'

    # print(f"Blob name: {blob_name}")
    # blob_client = blob_resume_container_client.get_blob_client(blob_name)

    #try:
    #    download_stream = blob_client.download_blob()
    #    file_content = download_stream.readall()
    #
    #    response = make_response(file_content)
    #    response.headers['Content-Type'] = 'application/pdf'
    #   return response
    #except Exception as e:
    #    print(f"Error downloading file: {str(e)}")
    #    return make_response('Failed to download file', 500)


@app.route('/resume', methods=['GET'])
def get_resume():
    resume_name = request.args.get('resumeName')
    print(f"Input: {resume_name}")
    
    # Get blob client (assuming blob_resume_container_client is initialized elsewhere)
    if "tailored" in resume_name.lower():
        blob_client = blob_resume_container_client.get_blob_client(resume_name)
    else:
        blob_client = blob_resume_container_client.get_blob_client("processed/" + resume_name)

    try:
        # Download the blob in binary mode
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall()
        
        # Convert the DOCX content to HTML using mammoth
        with io.BytesIO(file_content) as docx_stream:
            html_content = PyDocX.to_html(docx_stream)
            #result=mammoth.convert_to_markdown(docx_stream)
            #html_content = result.value  # The generated HTML content
            
        
        # Return as an HTML response
        response = make_response(html_content)
        response.headers['Content-Type'] = 'text/html'
        return response

    except Exception as e:
        print(f"Error downloading or processing file: {str(e)}")
        return make_response('Failed to download or process file', 500)



        
        

    
@app.route('/download', methods=['GET'])
def download_resume():
    resume_name = request.args.get('resumeName')

    if resume_name.startswith('Tailored/'):
        # For enhanced resumes, get the PDF
        blob_name = resume_name
        content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    else:
        # For regular resumes, get the DOCX from the processed folder
        blob_name = 'processed/' + resume_name
        content_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    blob_client = blob_resume_container_client.get_blob_client(blob_name)
    
    try:
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall()

        response = make_response(file_content)
        response.headers['Content-Type'] = content_type
        response.headers['Content-Disposition'] = f'attachment; filename="{resume_name}"'
        return response
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        return make_response('Failed to download file', 500)

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
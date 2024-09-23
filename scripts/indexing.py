
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchFieldDataType,
    SearchableField,
    SearchField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticConfiguration,
    SemanticPrioritizedFields,
    SemanticField,
    SemanticSearch,
    SearchIndex
)
from datetime import datetime, timezone
import json
import hashlib
from typing import Any
import PyPDF2


from azure.core.credentials import AzureKeyCredential  
from azure.search.documents import SearchClient  
from datetime import datetime
import os  
from dotenv import load_dotenv  
from azure.core.credentials import AzureKeyCredential  

from openai import AzureOpenAI  
import os
from langchain_openai import AzureChatOpenAI
import itertools


from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult


from azure.core.credentials import AzureKeyCredential

import tiktoken
from dotenv import load_dotenv 
import requests
import pandas as pd

load_dotenv()

connect_str = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
container_name = "resume-data"
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")

form_recognizer_endpoint = os.getenv("FORM_RECOGNIZER_ENDPOINT")
form_recognizer_key = os.getenv("FORM_RECOGNIZER_KEY")

ai_search_endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
ai_search_key = os.environ["AZURE_SEARCH_KEY"]
ai_search_index = os.environ["AZURE_SEARCH_INDEX"]

# Azure OpenAI
aoai_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
aoai_key = os.getenv("AZURE_OPENAI_API_KEY")
aoai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")

endpoint = form_recognizer_endpoint
credential = AzureKeyCredential(form_recognizer_key)
document_intelligence_client = DocumentIntelligenceClient(endpoint, credential)


search_index_client = SearchIndexClient(ai_search_endpoint, AzureKeyCredential(ai_search_key))
search_client = SearchClient(ai_search_endpoint, ai_search_index, AzureKeyCredential(ai_search_key))

aoai_client = AzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT"), 
        api_key=os.getenv("AZURE_OPENAI_KEY"),  
        api_version="2023-05-15"
        )



primary_llm = AzureChatOpenAI(
    azure_deployment=aoai_deployment,
    api_version="2024-05-01-preview",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=aoai_key,
    azure_endpoint=aoai_endpoint
)

primary_llm_json = AzureChatOpenAI(
    azure_deployment=aoai_deployment,
    api_version="2024-05-01-preview",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=aoai_key,
    azure_endpoint=aoai_endpoint,
    model_kwargs={"response_format": {"type": "json_object"}}
)

resume_indexing_prompt = """You are an AI assistant. Your job is to read the input resume, 
and output certain info in valid JSON format. Here is what you should be extracting:

1. experienceLevel - years of experience (an integer)
2. skills_and_experience - a succinct list of 3-5 top skills and experiences.   


#Examples#

User: Dan Giannone
CAREER SUMMARY
Resourceful and detail-oriented general contractor with 5+ years of experience recruiting and coordinating labor across construction sites.
Adept at obtaining permits, inspecting sites, ensuring building code compliance, offering cost estimates, and employing skilled labor.
Articulate communicator and effective negotiator with the ability to foster strong relationships with organizational management, key clients, vendors, and team members.
PROFESSIONAL EXPERIENCE
March 2020 - Present | Precision Pro 5, New York, NY
General Contractor
· Manage various construction projects with budgets of up to $5M each
· Train subcontractors on company standards and protocols
· Attained high client satisfaction by optimizing project efficiency and ensuring timely completion
· Achieved annual cost control targets in 2020 and 2021 through strategic planning and execution
June 2017 - March 2020 | YPC, New York, NY
General Contractor
· Performed pre-construction inspections and managed post-
construction audits for 3+ projects per year
· Streamlined project-related functions by developing schedules,
overseeing quality control, and ensuring within-budget project
completion
· Reported progress and project modifications to superintendents
and clients
· Supervised team of 6 subcontractors to resolve complex issues
and prevent unnecessary delays
(917) 828-9045
eloise.plaza@email.com
in
linkedin.com/in/eloise-plaza/
EDUCATION
Bachelor of Science in
Construction Management
Honors: cum laude (3.6/4.0)
Columbia University,
New York, NY
May 2017
SKILLS
Cost reduction & elimination
Project estimation
Residential construction
House renovation & remodeling
Subcontractor management
Workforce planning & scheduling
Complex problem-solving
Contract negotiation
Microsoft Office
Google Suite


Assistant: {'experienceLevel': '5', 'skills_and_experience': ['Cost reduction & elimination', 'Project estimation', 'Subcontractor management', 'Workforce planning & scheduling', 'Contract negotiation']}

"""

def generate_embeddings(text, model="embedding-ada"): # model = "deployment_name"
    return aoai_client.embeddings.create(input = [text], model=model).data[0].embedding

def get_creation_date(pdf_file):

        print(f"Attempting to open {pdf_file}")
        with open(pdf_file, 'rb') as f:
            pdf = PyPDF2.PdfReader(f)
            info = pdf.metadata
            print(info)
            raw_date = info.get('/CreationDate')

            # Strip the 'D:' prefix if it exists and extract only the necessary part (first 14 characters for YYYYMMDDHHMMSS)  
            pdf_date_str = raw_date[2:]  
            pdf_date_str = pdf_date_str[:14]  # Keep only the date and time part YYYYMMDDHHMMSS  
        
            
            pdf_datetime = datetime.strptime(pdf_date_str, '%Y%m%d%H%M%S')  
             
            # Format the datetime object to include microseconds (zeroes) and a 'Z' for UTC  
            return pdf_datetime.strftime("%Y-%m-%dT%H:%M:%S.000000Z") 





def create_index():

   

    
    #Check if index exists, return if so
    try:
        # Try to get the index
        search_index_client.get_index(ai_search_index)
        # If no exception is raised, the index exists and we return
        print("Index already exists")
        return
    except:
        # If an exception is raised, the index does not exist and we continue with the logic to create it
        pass

    # Rest of your code...

    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
        SimpleField(name="date", type=SearchFieldDataType.DateTimeOffset, filterable=True, facetable=True),
        SearchableField(name="jobTitle", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SimpleField(name="experienceLevel", type=SearchFieldDataType.Int32, filterable=True, facetable=True),  # Updated to Int32
        SearchableField(name="content", type=SearchFieldDataType.String),
        SearchableField(name="sourceFileName", type=SearchFieldDataType.String, filterable=True),
        SearchField(name="searchVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    searchable=True, vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile"),
        SearchableField(name="employee_first_name",searchable=True,type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="employee_last_name",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="location",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="employee_number",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="employee_Title",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="certifications",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="skills",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="job_family",type=SearchFieldDataType.String, filterable=True,facetable=True),
        SearchableField(name="primary_practice",type=SearchFieldDataType.String, filterable=True,facetable=True)]



    vector_search = VectorSearch(
    algorithms=[
        HnswAlgorithmConfiguration(
            name="myHnsw"
        )
    ],
    profiles=[
        VectorSearchProfile(
            name="myHnswProfile",
            algorithm_configuration_name="myHnsw",
        )
    ]
)
    index = SearchIndex(name=ai_search_index, fields=fields,
                    vector_search=vector_search)
    result = search_index_client.create_or_update_index(index)

    print("Index has been created")

def read_pdf(input_file):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    inp_blob_file_content=blob_service_client.get_container_client(container_name).download_blob(input_file).readall()
    # Start the analysis
    poller = document_intelligence_client.begin_analyze_document(
        model_id="prebuilt-layout",
        analyze_request = {
                    "base64Source": inp_blob_file_content  # Optional. Base64 encoding of the document to analyze.  Either urlSource or base64Source must be specified.
                }
    )
    result: AnalyzeResult = poller.result()
    #print(result.content)
    
    

    #read result object into a full text variable
    full_text = result.content
    print("Successfully read the PDF from blob storage with doc intelligence and extracted text.")
    
    return full_text

def llm_extraction(full_text):

    messages = [{"role": "system", "content": resume_indexing_prompt}]
    messages.append({"role": "user", "content": full_text})

    response = primary_llm_json.invoke(messages)
    extraction_json = json.loads(response.content)


    return extraction_json

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import uuid

def generate_document_id(blob_name):
    """Generate a unique, deterministic ID for a document."""
    unique_string = f"{blob_name}"  # Use first 100 characters of content for uniqueness
    return hashlib.md5(unique_string.encode()).hexdigest()

def list_blobs_in_folder(container_client, folder_name):
    return [blob for blob in container_client.list_blobs() if blob.name.startswith(folder_name)]

def move_blob(source_container_client, destination_container_client, source_blob_name, destination_blob_name):
    source_blob = source_container_client.get_blob_client(source_blob_name)
    destination_blob = destination_container_client.get_blob_client(destination_blob_name)
    
    destination_blob.start_copy_from_url(source_blob.url)
    source_blob.delete_blob()

def populate_index():
    print("Populating index...")
    
    # Read the metadata Excel file into a DataFrame
    metadata_df = pd.read_excel("scripts//EmailListBuilder_modified.xlsx")
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    stage_blobs = list_blobs_in_folder(container_client, "source/")

    # Subsetting to 100 resumes.
    stage_blobs = stage_blobs[:100]
    print(f"Found {len(stage_blobs)} blobs in the 'source' folder")
    
    for blob in stage_blobs:
        print(f"Processing {blob.name}")
        
        try:
            # Extract employee ID from the blob name (assuming the format is NAME_ID.extension)
            blob_name_parts = os.path.basename(blob.name).split(' ')
            employee_id = blob_name_parts[-1].split('.')[0]  # Adjust this depending on your naming convention
            print(f"Extracted employee ID: {employee_id}")
            
            # Find the corresponding metadata row for this employee ID
            metadata_row = metadata_df[metadata_df["EMPLOYEE_NUMBER"] == int(employee_id)].fillna("")
            
            #if metadata_row.empty:
             #   print(f"No metadata found for employee ID: {employee_id}")
             #   continue
            
            # Extract metadata columns (example: name, department, etc.)
            employee_first_name = metadata_row["EMP_FNAME"].values[0]
            employee_last_name = metadata_row["EMP_LNAME"].values[0]
            location = metadata_row["LOCATION"].values[0]
            employee_number = metadata_row["EMPLOYEE_NUMBER"].values[0].astype(str)
            employee_Title = metadata_row["TITLE"].values[0]
            certifications = metadata_row["CERTIFICATION"].values[0]
            skills = metadata_row["SKILL"].values[0]
            job_family = metadata_row["JOB_FAMILY"].values[0]
            primary_practice = metadata_row["PRIMARY_PRACTICE"].values[0]
            
            # Read the full text of the resume
            full_text = read_pdf(blob.name)
            extraction_json = llm_extraction(full_text)

            # Extract relevant fields from LLM extraction
            experienceLevel = extraction_json["experienceLevel"]
            #jobTitle = extraction_json["jobTitle"]
            skills_and_experience = extraction_json["skills_and_experience"]
            skills_and_experience_str = ", ".join(skills_and_experience)
            searchVector = generate_embeddings(skills_and_experience_str)
            current_date = datetime.now(timezone.utc).isoformat()
            document_id = generate_document_id(blob.name)
            fileName = os.path.basename(blob.name)
            
            # Combine extracted fields and metadata into the document
            document = {
                "id": document_id,
                "date": current_date,
                "jobTitle": employee_Title,
                "experienceLevel": experienceLevel,
                "content": full_text,
                "sourceFileName": fileName,
                "searchVector": searchVector,
                "employee_first_name":employee_first_name,
                "employee_last_name":employee_last_name,
                "location":location,
                "employee_number":employee_number,
                "certifications":certifications,
                "skills":skills,
                "job_family":job_family,
                "primary_practice":primary_practice
            }
            
            # Upload the document to the search index
            search_client.upload_documents(documents=[document])
            
            # Move the processed file to the 'processed' folder
            #destination_blob_name = blob.name.replace("source/", "processed/")
            #move_blob(container_client, container_client, blob.name, destination_blob_name)
            
            print(f"Successfully processed and moved {blob.name}")
        
        except Exception as e:
            print(f"Error processing {blob.name}: {str(e)}")


def reset_processed_files():
    """Move all files from the 'processed' folder back to the 'source' folder."""
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    processed_blobs = list_blobs_in_folder(container_client, "processed/")
    
    for blob in processed_blobs:
        source_blob_name = blob.name
        destination_blob_name = source_blob_name.replace("processed/", "source/")
        
        try:
            # Move the blob back to the 'source' folder
            move_blob(container_client, container_client, source_blob_name, destination_blob_name)

        except Exception as e:
            print(f"Error moving {source_blob_name} back to 'source': {str(e)}")

if __name__ == "__main__":

    #reset_processed_files()

    create_index()

    populate_index()

    

    
    


    


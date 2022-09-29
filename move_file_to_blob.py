from azure.storage.blob import ContainerClient
import os
import shutil
import yaml

def load_config():
    dir_root=os.path.dirname(os.path.abspath(__file__))
    with open(dir_root+"\\config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def get_files(file_path):
    with os.scandir(file_path) as paths:
        for path in paths:
            if path.is_file() and not path.name.startswith("."):
                yield

def move_file(path, destination):
    shutil.move(path,destination)
    return f"{path} has been moved"




def blob_upload(files, connection_string, container_name):
    container_client=ContainerClient.from_connection_string(connection_string,container_name)
    print("Uploading files to Azure blob storage")
    for file in files:
        blob_client=container_client.get_blob_client(file.name)
        with open(file.path, "rb") as data:
            try:
                blob_client.upload_blob(data)
                print(f"{file.name} uploading to blob storage")
                move_file(file, )
            except:
                print(f"{file.name} failed to upload")



config=load_config()
source_files=get_files(config["source_folder"])
print(*source_files)

config=load_config()
print(config['azure_blob_connectionstring'])

import requests, json, os
import xml.etree.ElementTree as ET

# url to xml file that contain source files
url = "http://udacity-dend.s3.amazonaws.com/"

response = requests.get(url)

# write the contents to xml file
with open('feed.xml', 'wb') as file:
    file.write(response.content)

# parse the xml file to get all the source files
tree = ET.parse('feed.xml')
root = tree.getroot()

for child in root:
    for file in child:
        # key contains the path to source file
        if 'Key' in file.tag:
            if '.json' in file.text and ('song-data' in file.text or 'log-data' in file.text):
                response = requests.get(url + file.text)

                # Print Files
                # print(file.text, response.text)

                # Check if path already exists if it does not create one
                if not os.path.exists(os.path.dirname(file.text)):
                    os.makedirs(os.path.dirname(file.text))
                with open(file.text, 'wb') as output_file:
                    output_file.write(response.content)

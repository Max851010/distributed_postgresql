#gcloud auth login
cd master_server && pip3 freeze > requirements.txt && cd ..
cd update_server && pip3 freeze > requirements.txt && cd ..
cd client && pip3 freeze > requirements.txt && cd ..
docker build -t master-server ./master_server
docker build -t update-server ./update_server
docker build -t client ./client
docker tag master-server gcr.io/cse-512-443000/master-server
docker tag update-server gcr.io/cse-512-443000/update-server
docker tag client gcr.io/cse-512-443000/client
docker push gcr.io/cse-512-443000/client
docker push gcr.io/cse-512-443000/master-server
docker push gcr.io/cse-512-443000/update-server

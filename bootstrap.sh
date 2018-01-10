# Pre-build images
docker-compose up --no-start
# How the hell do we distribute images across swarm?
docker stack deploy --compose-file=docker-compose.yml lab3

# Lightweight alpine OS, weight only 5mb, everything else is Go # # environment
FROM golang:1.9.2

# Setup environment variables
ENV GCS_BUCKET discovery-post-images
ENV ES_URL http://35.230.14.141:9200/

# Define working directory
WORKDIR /go/src/github.com/einiemand/discovery


# Add files from your laptop to inside the docker image
ADD . /go/src/github.com/einiemand/discovery

# Install dependencies
RUN go get -v \
cloud.google.com/go/bigtable \
cloud.google.com/go/storage \
github.com/auth0/go-jwt-middleware \
github.com/dgrijalva/jwt-go \
github.com/go-redis/redis \
github.com/gorilla/mux \
github.com/olivere/elastic \
github.com/pborman/uuid \
github.com/pkg/errors \
golang.org/x/oauth2/google

# Expose port
EXPOSE 8080

# Entrypoint
CMD ["C:/Golang/bin/go", "run", "main.go" ]
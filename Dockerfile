# build
FROM 652969937640.dkr.ecr.us-east-1.amazonaws.com/containers/golang-1.14:production
WORKDIR /build
COPY . /build
RUN mkdir bin
RUN go build -o bin/redisbetween .

# deploy
FROM 652969937640.dkr.ecr.us-east-1.amazonaws.com/containers/golang-1.14:production
WORKDIR /app
COPY --from=0 /build/bin/redisbetween ./

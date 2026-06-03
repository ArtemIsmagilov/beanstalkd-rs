FROM alpine
RUN apk add --no-cache beanstalkd
ENTRYPOINT ["beanstalkd"]

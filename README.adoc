= Redis Cluster Manager

This is a cluster manager implementation for Vert.x that uses https://github.com/redisson/redisson/[Redisson].

https://github.com/leotu/vertx-cluster-redis

- Need to refactoring the code
- Upgrade to Vert.x 3.8.1

== Test docker
 
[source,bash]
----
docker pull redis:alpine
docker run --name demo.redis -e LANG=en.UTF-8 -e LC_ALL=en.UTF-8 -p 6379:6379 -d redis:alpine --requirepass "mypwd"
----

Tip: Useful commands:

----
docker container ls
docker image ls (or docker images)
docker container rm xxx
----
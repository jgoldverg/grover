# Whats the Point of this you will probably ask?? Not sure


## The overarhcing goal of this work

There are 3 cases for file transfers:
Downloading/uploading using the client to a supported endpoint or the server here.
Moving data between two servers that you launch, just hit one with the pb command or maybe i could write some cli support for this. Overlay network
Multiple server based file transfers.

Tbh I have no idea how I can construct such a system. Look at it, client -> server or client <- server is simple more or less. Nothing crazy to say the least but when you start optimizing it heavily:
1. Parallel chunk sending per file
2. Multiple file transfers in parallel
3. Somehow get zero-copy
4. Write the UDP protocol
5. Try and use eBPF for the server with XDP
6. We need to include metrics reporting as well: network metrics that we drive up via eBPF from the server side.
7. Can we utilize multiple nic's if they are available as well to split traffic across?
Hence the expectation this is a very slow burn of a project and one where I just simply play around with and try build something that craps on onedatashare and globus.


## Major things to work on still

### cli
There are many commands to add but to summarize we should support min: list(done), mkdir, touch, rename, move, transfer, sync, stream

These are just implementing the calls to backend using cobra but are crucial for actually doing integration testing and see how the protocol works.

All are simple aside from:
move - is simply moving a directory on the local file system like the Unix command.
sync - means that as changes to a file they are reflected on the upstream server, or the opposite.
transfer - an actual data transfer. It gets complicated when you want to do a broadcast or a gather of various files from various servers. Not sure how we are gonna do this but I think its just figuring out to associate together parts of file transfer to different servers. Hence we really call these two cases scatter and gather.

stream - means that we constantly read from a file or a socket in general and push that data to a destination. This I haven't specifically thought about much but if we have a directory with a file that gets say log entries every second we want to read and push that data deleting the original.

Management commands:
- credentials: here there isnt much for the Toml format of credentials but we can for example add encryption or password or jwt access.

### backend
This is where we have protocol implementations that the client's(cli for ex) and server will use to conduct the operations. It completely encapsulates all protocol based code, we really only have the concet of readers and writers and operating on chunks of data, we can include support for stream operations as well.

### server
This is far more complicated and writing is confusing hence I need to iron this out more.
There are a few servers in reality: information channel to the server is for all operations like(transfer, ls, rm,,, etc) we want this over grpc as its by far the simplest thing to use. I am thinking that we actually implement the grpc server and a custom protocol server as well.

### network focus

Ugh man in so many ways there are tons of protocols to use with various ideas, problem is I am sick of not having exactly want. A higher performance protocol that supports chunking, striping out of order packets, that does proper monitoring of itself. Using things like ftp, scp,,, leave you completely blind of whats going in the network and making certain decisions up front.

Decisions that we can consider:
1. In the overlay network scenario we can do tons of routing decisions that I think would be fun to implement in the user space
2. No eBPF support in tons of protocol servers when tbh we just want traffic to come in as fast as possible.
3. Lets see if we can do a zero copy approach for the client and server, but that would be a fun challenge to make a protocol server and client as fast as bleeding possible.
4. There are just tons of tricks you can do in networking that many file transfer protocols simply dont support and its killing me at this point. Whats crazy is http tends up being the most performant but is still extremely lacking when looking at performance.

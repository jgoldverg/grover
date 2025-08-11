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


## Project Organization

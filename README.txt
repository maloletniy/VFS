Please design and implement a library to emulate the file system, consisting of directory tree and paths,
 which stores all data in one single file.

Create-write-read-append-delete-rename-move operations should be supported.
Keep balance between container file size and CPU resources.
Provide a unit tests for all features, with coverage report.

Start with a design overview and estimates. 
Log all your work, decisions and problems encountered in this document 
Append dated sections only.
Please use English only.
Conclude with performance and scalability analysis of final solution (CPU/RAM/DISK).

Store project at GitHub or BitBucket git repo, include link here.
This should be an IDEA project with working “all tests” run config ready to run on any os immediately after checkout.
Do not use any libs except unit test libs and logging libs.

Time is not limited, but you need to follow your own estimate or provide new.
Be ready to answer questions on design, implementation and amending your code at final interview.


cluster size(512b)
node:
name: root
type: d
links:
{
name: text.txt
type: f
data: 1024 - 4 block
,
name: bin
type: d
links: {
        name: hello.bin
        data:2048 - 8 block
        }
}
(node size)+0:
(node size)+512:
(node size)+1024: gjggjgjgjgjfjjef,-1  |512b
(node size)+1536:
(node size)+2048: addwaawfawfaw,-1     |512b


cluster(default 1024b):
    1 byte full\empty
    8 bytes link to next cluster
    1015 bytes data

node:
       256 subnodes (long)  \ 256 pointer to data (long)  |    8b * 256
       name (char[16])                                         2b*16
       type (char)                                             2b
       size (long)                                             4b
       node size: 2086bytes

Welcome to albertdb.
This is a Key-Value database I wrote inspired by other LSM databases like Scylla, Dynamo and RocksDB.
I did this to have a project to work on while learning rust, 
    but also to demonstrate that I have some competency in database development.

The purpose of this document is to give an overview of the database and how it works.

The database uses quite a few different data structures such as sstables, memtables, bloomfilters.
The structure and performance charactaristics of each of these won't be dissucssed in detail
  in this document.
This document is an instruction for how each of them fit together.

# Write Path

Writes receive the input as a Key/Value pair where both the key and the value are arbitrary
    arrays of bytes.

The writes are immediately peristed to a _memtable_ which is an in-memory balanced tree (
    balanced from the perspective of the _key_).
<!-- TODO insert a picture of a tree  that is balanced by key-->

The writes are also immediately peristed to write ahead log (WAL),
    which stores the key/value records on disk.
The WAL will be used to recover the memtable in cases where the database crashes.

memtable cannot grow indefinitely b/c it would use all memory on system.
When the memtable (and corresponding WAL) reach a threshold,
    the memtable is flushed to an on-disk file called an sstable.
After persisting the sstable to disk,
    the WAL is also deleted as it is no longer needed.
The memtable remains available for reads during the flushing process.


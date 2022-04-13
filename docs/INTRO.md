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

### Memtable Flush
memtable cannot grow indefinitely b/c it would use all memory on system.
When the memtable (and corresponding WAL) reach a threshold,
    the memtable is flushed to an on-disk file called an sstable.
After persisting the sstable to disk,
    the WAL is also deleted as it is no longer needed.

Once an sstable begins to be flushed,
    a new sstable will be created that is available to write to.
Only one memtable will be writable at a time.
The memtable being flushed remains available for reads during the flushing process.

Once flushing completes,
    sstables are immutable and they become readable.

### Updates

Updates happen differently depending on where the old data was.
If the older record was in the memtable, the node in the memtable (balanced tree)
    is replaced.

If the older record has been flushed to an sstable, 
    we just write the value into the tree.
The read path takes care of reading from the memtable before it reads sstable.

## Read Path

There is a sequence of datastructures that could contain the record.
Each one will be checked in the order of how new are the values that each contains.

First will be checked the writable memtable.
Then will be checked each flushing memtable in order of how newly they were created.
Finally each disk-resident sstable will be checked in order of their newness.


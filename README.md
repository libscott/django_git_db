# Git DB Backend for Django

An (incomplete) RDBMS database backend for Django.

## Goals

* Enable easy experimentation for using a [merkel dag](https://github.com/ipfs/specs/tree/master/merkledag) as a database backend.
* Provide a Django compatible interface so people can play with it using their existing apps at near zero setup cost.
* Provide a way for a user to branch data within a session and merge it back later (long lived transaction)
* Support IPFS as a storage backend

## What works

* Parsing and execution of SQL DDLs (CREATE TABLE etc)
* _very_ basic execution of Django query objects (not SQL statements), and without indexes
* Transactions

## What doesn't work

* Everything else

## Testing

`runtests.sh` runs the Django test suite using GIT as a database backend. All the initial migrations (over 1400 tables) complete.

## Project status

The following things have become clear:

* You CAN map a relational database onto a Merkel dag and it would probably be quite fun / interesting to do so.
* Using GIT with the filestem as a storage backend, this has HORRIBLE space usage:
    - Every update to a tree will create log(n) new tree objects
    - Each tree object will have a separate file and therefore take up $block_size (4k) at a minimum
    - Running the test suite migrations alone uses 2GB of space
    - Would need a proper garbage collector for this to be really useful.

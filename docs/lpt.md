# Load testing

We've performed various load tests for corvus, using
both `redis-benchmark` and an in-house test framework.

The latter is not open sourced, so we'll only publish tests
done with `redis-benchmark`.

## Hardware

* CPU: 2 x Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz 
* Memory: 32G
* NIC: 2G (2 x Intel Corporation I350 Gigabit Network Connection)

## Software

* Linux 3.10.0-229.el7.x86_64 #1 SMP Fri Mar 6 11:36:42 UTC 2015 x86_64 x86_64 x86_64 GNU/Linux
* LSB Version: :core-4.1-amd64:core-4.1-noarch
* Distributor ID: CentOS
* Description: CentOS Linux release 7.1.1503 (Core)
* Release: 7.1.1503
* Codename: Core

## Deployment

Corvus(0.2.4): 1 dedicated server with 24 thread.  
Redis(3.0.3): 3 dedicated server with 60 nodes, in which 30 are masters.


## Tests

Note: The following tests are carried out in extreme load, focused
on how much load it can operate on, in disregard of service quanlity(return time).


1. [set](redis_benchmarks/set.md)
2. [get](redis_benchmarks/get.md)
3. [mset](redis_benchmarks/mset.md)

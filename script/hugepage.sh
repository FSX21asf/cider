#!/bin/bash
sysctl -w vm.nr_hugepages=80000
ulimit -l unlimited
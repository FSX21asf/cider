#!/bin/bash
sysctl -w vm.nr_hugepages=65536
ulimit -l unlimited
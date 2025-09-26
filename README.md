# CIDER: Boosting Memory-Disaggregated Key-Value Stores with Pessimistic Synchronization


This is the implementation repository of our paper: **CIDER: Boosting Memory-Disaggregated Key-Value Stores with Pessimistic Synchronization**.



- [CIDER: Boosting Memory-Disaggregated Key-Value Stores with Pessimistic Synchronization](#cider-boosting-memory-disaggregated-key-value-stores-with-pessimistic-synchronization)
  - [Supported Platform](#supported-platform)
  - [Create Cluster](#create-cluster)
  - [Source Code](#source-code)
  - [Environment Setup](#environment-setup)
  - [Getting Started](#getting-started)


## Supported Platform
We strongly recommend you to run CIDER using the r650 instances on [CloudLab](https://www.cloudlab.us/) as the code has been thoroughly tested there.
We haven't done any test in other hardware environment.

## Create Cluster

Follow the steps below to create an experimental cluster with 8 r650 nodes on CloudLab:

1) Log into your own account.

2) Click `Experiments`|-->`Create Experiment Profile`. Upload `./script/cloudlab.profile` provided in this repo.
Input a file name (*e.g.*, CIDER) and click `Create` to generate the experiment profile for CIDER.

3) Click `Instantiate` to create a 8-node cluster using the profile
   

## Source Code
Now you can log into all the CloudLab nodes. Using the following command to clone this github repo in the home directory (*e.g.*, /users/TempUser) of **all** nodes:
```shell
git clone https://github.com/FSX21asf/cider.git
```


## Environment Setup

You have to install the necessary dependencies in order to build CIDER.
Note that you should run the following steps on **all** nodes you have created.

1)  Enter the CIDER directory.
    ```shell
    cd cider
    ```

2) Install Mellanox OFED.
    ```shell
    sudo sh script/installMLNX.sh
    ```


3) Install the required libraries.
    ```shell
    sudo sh script/installLibs.sh;
    ```

4) Reboots.
    ```shell
    reboot
    ```
You can reboot all nodes in the cluster via your experiment in CloudLab|-->List View|-->Reboot Selected

## Getting Started

* Hugepages setting on **all** nodes (This step should be re-execute every time you reboot the nodes).
    ```shell
    cd cider
    sudo sh script/hugepage.sh
    ```

* Return to the root directory of CIDER and execute the following commands on **all** nodes to compile CIDER:
    ```shell
    mkdir -p build; cd build; cmake -DSTATIC_MN_IP=on -DENABLE_CORO=OFF -DUSE_MCS_WCV=ON -DUSE_ST=ON -DDELAY_WRITE=ON ..; make -j
    ```

* Execute the following command on **one** node to initialize the memcached:
    ```shell
    sudo bash ../script/restartMemc.sh
    ```

* Execute the following command on **all** nodes to conduct a zipfian evaluation on the pointer array:
    ```shell
    ./array_zipfian_test <CN_num> <read_ratio> <client_num_per_CN> <zipfian> 1 
    ```
 
    **Example**:
    ```shell
    ./array_zipfian_test 8 50 64 0.99 1
    ```

* Results:
    * Throughput: the throughput of **CIDER** of the whole cluster will be shown in the terminal of the first node.
    * Latency: execute the following command on **one** node to calculate the latency results of the whole cluster:
        ```shell
        python3 ../us_lat/cluster_latency.py <CN_num> <epoch_start> <epoch_num>
        ```

        **Example**:
        ```shell
        python3 ../us_lat/cluster_latency.py 8 2 4
        ```

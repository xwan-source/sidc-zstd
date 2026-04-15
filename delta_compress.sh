#!/bin/bash

chro_path="/ssd/dataset/chrome/"
syn_path="/ssd-4t/dataset/bench_tar_usr002/"
rdb_path="/ssd/dataset/RDB/"
vmdk_path="/ssd-4t/dataset/vmdk/"
webs_tar_path="/ssd/dataset/webs_tar/"
linux_path="/ssd-4t/dataset/linux_tar/"

#path=$chro_path
#dataset="chro"
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir greedy_ram_$dataset/
#mv excel_* greedy_ram_$dataset/
#mv total.txt greedy_ram_$dataset/

#path=$linux_path
#dataset="linux"
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dataset_$dataset/
#mv excel_* dataset_$dataset/
#mv total.txt dataset_$dataset/

#path=$syn_path
#dataset="syn"
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dataset_$dataset/
#mv excel_* dataset_$dataset/
#mv total.txt dataset_$dataset/

path=$rdb_path
dataset="rdb"
./rebuild
for file in $(ls $path);do
    ./destor $path/$file
done

for ((i = 0; i <= 199; i++))
do
    ./destor -r$i /home/dataset/
    rm -rf /home/dataset/*

done
./destor -s > total.txt
mkdir dataset_restore_$dataset/
mv excel_* dataset_restore_$dataset/
mv total.txt dataset_restore_$dataset/
rm -rf /home/dataset/*

#path=$vmdk_path
#dataset="vmdk"
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dataset_$dataset/
#mv excel_* dataset_$dataset/
#mv total.txt dataset_$dataset/
#
#for ((i = 0; i <= 124; i++))
#do
#    ./destor -r$i /home/dataset/
#
#    if [ $((i % 10)) -eq 9 ]; then
#      rm -rf /home/dataset/*
#    fi
#done
#
#./destor -s > total.txt
#mkdir dataset_restore_$dataset/
#mv excel_* dataset_restore_$dataset/
#mv total.txt dataset_restore_$dataset/
#rm -rf /home/dataset/*


#path=$webs_tar_path
#dataset="webs_tar"
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dataset_$dataset/
#mv excel_* dataset_$dataset/
#mv total.txt dataset_$dataset/

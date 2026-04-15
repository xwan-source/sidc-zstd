#!/bin/bash

chro_path="/ssd/dataset/chrome/"
syn_path="/ssd-4t/dataset/bench_tar_usr002/"
rdb_path="/ssd/dataset/RDB/"
vmdk_path="/ssd-4t/dataset/vmdk/"
webs_tar_path="/ssd/dataset/webs_tar/"
linux_path="/ssd-4t/dataset/linux_tar/"
rdb_dump="/ssd-4t/dataset/rdb_dump/"

path=$webs_tar_path
dataset="web"
./rebuild
for file in $(ls $path);do
    echo 3 > /proc/sys/vm/drop_caches
    ./destor $path/$file

done

for ((i = 0; i <= 119; i++))
do
    ./destor -r$i /home/dataset/
    rm -rf /home/dataset/*

done
./destor -s > total.txt
mkdir test_clear_$dataset/
mv excel_* test_clear_$dataset/
mv total.txt test_clear_$dataset/

path=$rdb_path
dataset="rdb"
./rebuild
for file in $(ls $path);do
    echo 3 > /proc/sys/vm/drop_caches
    ./destor $path/$file

done

for ((i = 0; i <= 199; i++))
do
    ./destor -r$i /home/dataset/
    rm -rf /home/dataset/*

done
./destor -s > total.txt
mkdir test_clear_$dataset/
mv excel_* test_clear_$dataset/
mv total.txt test_clear_$dataset/

path=$chro_path
dataset="chro"
./rebuild
for file in $(ls $path);do
    echo 3 > /proc/sys/vm/drop_caches
    ./destor $path/$file

done

for ((i = 0; i <= 99; i++))
do
    ./destor -r$i /home/dataset/
    rm -rf /home/dataset/*

done
./destor -s > total.txt
mkdir test_clear_$dataset/
mv excel_* test_clear_$dataset/
mv total.txt test_clear_$dataset/

path=$linux_path
dataset="linux"
./rebuild
for file in $(ls $path);do
    echo 3 > /proc/sys/vm/drop_caches
    ./destor $path/$file

done

for ((i = 0; i <= 299; i++))
do
    ./destor -r$i /home/dataset/
    rm -rf /home/dataset/*

done
./destor -s > total.txt
mkdir test_clear_$dataset/
mv excel_* test_clear_$dataset/
mv total.txt test_clear_$dataset/

#path=$webs_tar_path
#dataset="web"
#echo 3 > /proc/sys/vm/drop_caches
#./rebuild
#for file in $(ls $path);do
#
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dedup_$dataset/
#mv excel_* dedup_$dataset/
#mv total.txt dedup_$dataset/

#path=$rdb_dump
#dataset="rdb_dump"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#
#done
#
#for ((i = 0; i <= 199; i++))
#do
#    ./destor -r$i /home/dataset/
#    rm -rf /home/dataset/*
#
#done
#
#./destor -s > total.txt
#mkdir dedup_clear_$dataset/
#mv excel_* dedup_clear_$dataset/
#mv total.txt dedup_clear_$dataset/
#rm -rf /home/dataset/*

#for ((i = 0; i <= 99; i++))
#do
#    ./destor -r$i /home/dataset/
#    if [ $((i % 10)) -eq 9 ]; then
#        rm -rf /home/dataset/*
#    fi
#
#done

#path=$chro_path
#dataset="chro"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#
#done
#
##for ((i = 0; i <= 99; i++))
##do
##    ./destor -r$i /home/dataset/
##    if [ $((i % 10)) -eq 9 ]; then
##        rm -rf /home/dataset/*
##    fi
##
##done
#./destor -s > total.txt
#mkdir dedup_clear_$dataset/
#mv excel_* dedup_clear_$dataset/
#mv total.txt dedup_clear_$dataset/
#rm -rf /home/dataset/*
#
#path=$linux_path
#dataset="linux"
#./rebuild
#for file in $(ls $path);do
#      echo 2 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir test_$dataset/
#mv excel_* test_$dataset/
#mv total.txt test_$dataset/

#for ((i = 0; i <= 299; i++))
#do
#    ./destor -r$i /home/dataset/
#    if [ $((i % 10)) -eq 9 ]; then
#      rm -rf /home/dataset/*
#    fi
#done
#./destor -s > total.txt
#mkdir dedup_samplingindex_uniform_restore_$dataset/
#mv excel_* dedup_samplingindex_uniform_restore_$dataset/
#mv total.txt dedup_samplingindex_uniform_restore_$dataset/
#rm -rf /home/dataset/*
#
#path=$syn_path
#dataset="syn"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dedup_clear_$dataset/
#mv excel_* dedup_clear_$dataset/
#mv total.txt dedup_clear_$dataset/

#path=$webs_tar_path
#dataset="web"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#done
#
#for ((i = 0; i <= 119; i++))
#do
#    ./destor -r$i /home/dataset/
#    rm -rf /home/dataset/*
#
#done
#
#./destor -s > total.txt
#mkdir dedup_clear_$dataset/
#mv excel_* dedup_clear_$dataset/
#mv total.txt dedup_clear_$dataset/

#path=$linux_path
#dataset="linux"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#done
#./destor -s > total.txt
#mkdir dedup_clear_$dataset/
#mv excel_* dedup_clear_$dataset/
#mv total.txt dedup_clear_$dataset/

#for ((i = 0; i <= 179; i++))
#do
#    ./destor -r$i /home/dataset/
#    if [ $((i % 10)) -eq 9 ]; then
#      rm -rf /home/dataset/*
#    fi
#
#done
#./destor -s > total.txt
#mkdir dedup_samplingindex_uniform_restore_$dataset/
#mv excel_* dedup_samplingindex_uniform_restore_$dataset/
#mv total.txt dedup_samplingindex_uniform_restore_$dataset/
#rm -rf /home/dataset/*
#
#
#path=$rdb_path
#dataset="rdb"
#echo 3 > /proc/sys/vm/drop_caches
#./rebuild
#for file in $(ls $path);do
#    ./destor $path/$file
#done
#
#for ((i = 0; i <= 199; i++))
#do
#    ./destor -r$i /home/dataset/
#    rm -rf /home/dataset/*
#done
#./destor -s > total.txt
#mkdir IOs_$dataset/
#mv excel_* IOs_$dataset/
#mv total.txt IOs_$dataset/

#./destor -s > total.txt
#mkdir dedup_samplingindex_uniform_restore_$dataset/
#mv excel_* dedup_samplingindex_uniform_restore_$dataset/
#mv total.txt dedup_samplingindex_uniform_restore_$dataset/
#rm -rf /home/dataset/*

#path=$vmdk_path
#dataset="vmdk"
#./rebuild
#for file in $(ls $path);do
#    echo 3 > /proc/sys/vm/drop_caches
#    ./destor $path/$file
#
#done
#
#for ((i = 0; i <= 124; i++))
#do
#    ./destor -r$i /home/dataset/
#    rm -rf /home/dataset/*
#
#done
#./destor -s > total.txt
#mkdir dedup_restore_$dataset/
#mv excel_* dedup_restore_$dataset/
#mv total.txt dedup_restore_$dataset/

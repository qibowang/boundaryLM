#!/bin/bash

input=/user/wangqibo/20180205_mid_seg/middleLM
output=/user/wangqibo/20180205_mid_seg/middleLM_sort

hadoop jar middleSegSort.jar -input $input -output $output
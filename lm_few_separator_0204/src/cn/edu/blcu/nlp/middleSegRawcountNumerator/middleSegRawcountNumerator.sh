#!/bin/bash
startOrder=2
endOrder=6
tasks=1
isLzo=1
#input=/user/wangqibo/data/coupus/seg0/chuli_blog/049_.txt.txt_out
input=/user/wangqibo/data/coupus/seg0/lishijisuan_5/2013080113_02.txt.txt_out
output=/user/wangqibo/20180205_mid_seg/rawcount_numerator

hadoop jar middleSegRawcountNumerator.jar -input $input -output $output -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo 

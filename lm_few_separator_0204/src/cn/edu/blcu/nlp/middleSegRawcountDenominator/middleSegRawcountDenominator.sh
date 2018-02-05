#!/bin/bash
startOrder=1
endOrder=6
tasks=1
isLzo=1
#input=/user/wangqibo/data/coupus/seg0/chuli_blog/049_.txt.txt_out
input=/user/wangqibo/data/coupus/seg0/lishijisuan_5/2013080113_02.txt.txt_out
rawCountPath=/user/wangqibo/20180205_mid_seg/rawcount_denominator

hadoop jar middleSegRawcountDenominator.jar -input $input -rawcount $rawCountPath -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo 

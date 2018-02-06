#!/bin/bash

dataPath=/user/wangqibo/data/coupus/seg0/lishijisuan_5/2013080113_02.txt.txt_out
resParent=/user/wangqibo/20180206_mid_seg_20M
numeartorPath=$resParent/nume
denominatorPath=$resParent/deno
probPath=$resParent/prob
backPath=$resParent/back
backJoinPath=$resParent/backJoin
lmPath=$resParent/LM
lmSort=$resParent/LM_sort
startOrder=2
endOrder=6
tasks=1
isLzo=1

hadoop jar middleSegRawcountDenominator.jar -input $dataPath -rawcount $denominatorPath -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo 

hadoop jar middleSegRawcountNumerator.jar -input $dataPath -output $numeartorPath -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo

hadoop jar middleSegProb.jar -nume $numeartorPath -deno $denominatorPath -prob $probPath -isLzo $isLzo -tasks $tasks

hadoop jar middleSegBack.jar -input $numeartorPath -output $backPath -back $backJoinPath -tasks $tasks -isLzo $isLzo


hadoop jar middleSegProbJoinBack.jar -input $probPath -input $backPath -output $lmPath -tasks $tasks -isLzo $isLzo

hadoop jar middleSegSort.jar -input $lmPath -output $lmSort

#!/bin/bash

probPath=/user/wangqibo/20180205_mid_seg/prob
backPath=/user/wangqibo/20180205_mid_seg/middleBackJoin
lmPath=/user/wangqibo/20180205_mid_seg/middleLM
tasks=1
isLzo=1

hadoop jar -input $probPath -input $backPath -output $lmPath -tasks $tasks -isLzo $isLzo
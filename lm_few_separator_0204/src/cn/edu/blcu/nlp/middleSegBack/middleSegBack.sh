#!/bin/bash

input=/user/wangqibo/20180205_mid_seg/rawcount_numerator_Lzo
output=/user/wangqibo/20180205_mid_seg/middleBack
backPath=/user/wangqibo/20180205_mid_segmiddleBackJoin
tasks=1
isLzo=1

hadoop jar middleSegBack.jar -input $input -output $output -back $backPath -tasks $tasks -isLzo $isLzo
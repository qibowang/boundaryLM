#!/bin/bash

numePath=/user/wangqibo/20180205_mid_seg/rawcount_numerator_Lzo
denoPath=/user/wangqibo/20180205_mid_seg/rawcount_denominator_2_6_Lzo
probPath=/user/wangqibo/20180205_mid_seg/prob
tasks=1
isLzo=1

hadoop jar middleSegProb.jar -nume $numePath -deno $denoPath -prob $probPath -isLzo $isLzo -tasks $tasks
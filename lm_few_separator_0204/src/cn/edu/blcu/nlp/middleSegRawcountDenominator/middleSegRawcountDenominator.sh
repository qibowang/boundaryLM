#!/bin/bash
startOrder=1
endOrder=7
tasks=1
isLzo=1
input=
rawCountPath=

hadoop jar -input $input -rawcount $rawCountPath -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo

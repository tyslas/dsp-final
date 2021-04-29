#!/bin/bash

write_from=$1
write_to=$2
for i in {1..1000};do cat $write_from >> $write_to; done

cp /tmp/corauwash_master/corauwash-preloaded-without-blocks.mv.db /tmp/corauwash_master/corauwash-exp$1.mv.db
./run-master.sh --runid=$1 $3 $4 2>&1 | tee -a logs/Expt$1-DistCoraUWash.master.$2.log

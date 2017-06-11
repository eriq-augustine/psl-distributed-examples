cp /tmp/corauwash_worker/corauwash-preloaded-without-blocks.mv.db /tmp/corauwash_worker/corauwash-exp$1.mv.db
./run-worker.sh --runid=$1 $3 $4 2>&1 | tee -a logs/Expt$1-DistCoraUWash.worker.$2.log

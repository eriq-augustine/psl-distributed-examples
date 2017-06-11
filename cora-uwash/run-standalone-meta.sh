cp /tmp/corauwash_standalone/corauwash-preloaded-with-blocks.mv.db /tmp/corauwash_standalone/corauwash-exp$1.mv.db
./run-standalone.sh --runid=$1 $3 $4 2>&1 | tee -a logs/Expt$1-DistCoraUWash.standalone.$2.log

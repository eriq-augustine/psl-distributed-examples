cp /tmp/corauwash_master/corauwash-preloaded.mv.db /tmp/corauwash_master/corauwash-exp$1.mv.db
./run-master.sh --runid=$1 2>&1 | tee -a logs/Expt$1-DistCoraUWash.master.sozopol.log

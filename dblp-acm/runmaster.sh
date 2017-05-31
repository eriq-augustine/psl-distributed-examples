mvn compile
#./run.sh data Similarities.JWTFIDF.combo-only same_targets.txt same_truth.txt 15 --master 2>&1 | tee -a logs/Expt15-SingleDBLPACM-JWTFIDF.master.log
./run.sh data Similarities.L2JW.combo-only same_targets.txt same_truth.txt 16 --master 2>&1 | tee -a logs/Expt16-SingleDBLPACM-L2JW.master.log

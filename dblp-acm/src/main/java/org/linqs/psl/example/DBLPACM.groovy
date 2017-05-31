package org.linqs.psl.distrBibER;

import org.linqs.psl.application.inference.MPEInference;
import org.linqs.psl.application.inference.distributed.DistributedMPEInferenceMaster;
import org.linqs.psl.application.inference.distributed.DistributedMPEInferenceWorker;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DatabasePopulator;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.Queries;
import org.linqs.psl.database.ReadOnlyDatabase;
import org.linqs.psl.database.loading.Inserter;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.groovy.PSLModel;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.utils.dataloading.InserterUtils;
import org.linqs.psl.utils.evaluation.printing.AtomPrintStream;
import org.linqs.psl.utils.evaluation.printing.DefaultAtomPrintStream;
import org.linqs.psl.utils.evaluation.statistics.ContinuousPredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.DiscretePredictionStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.time.TimeCategory;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistBibERDBLPACM {
	private static final String PARTITION_OBSERVATIONS = "observations";
	private static final String PARTITION_TARGETS = "targets";
	private static final String PARTITION_TRUTH = "truth";

   private static final String ID = "dblpacm";

	private Logger log;
	private DataStore ds;
	private PSLConfig config;
	private PSLModel model;

	/**
	 * Class for config variables
	 */
	private class PSLConfig {
		public ConfigBundle cb;

		public String dbPath;
		public String dataPath;
		public String outputPath;

		public boolean sqPotentials;

		public distributed;
		public master;

		public String experimentName;
        public String simPath;
        public String targetsPath;
        public String truthPath;
        public String runid;

		public Map weightMap = [
			"Similar":20,
			"Transitivity":20,
			"Prior":1
		];
		public boolean useTransitivityRule = false;

		public PSLConfig(ConfigBundle cb) {
			this.cb = cb;

			distributed = cb.getBoolean('distributed', false);
			master = cb.getBoolean('master', false);

         String suffix = distributed ? (master ? "master" : "worker") : "standalone";
			dbPath = Paths.get(cb.getString('experiment.dbpath', '/tmp'), ID + "_" + suffix);
			dataPath = cb.getString('experiment.data.path', 'data');
			outputPath = cb.getString('experiment.output.outputdir', 'output');

			this.experimentName = cb.getString('experiment.name', 'default');
            this.simPath = cb.getString('experiment.similarity.path', 'something');
            this.targetsPath = cb.getString('experiment.targets.path', 'something');
            this.truthPath = cb.getString('experiment.truth.path','something');
			this.runid = cb.getString('experiment.runid', 'something');

			this.weightMap["Similar"] = cb.getInteger('model.weights.similar', weightMap["Similar"]);
			this.weightMap["Transitivity"] = cb.getInteger('model.weights.transitivity', weightMap["Transitivity"]);
			this.weightMap["Prior"] = cb.getInteger('model.weights.prior', weightMap["Prior"]);
			this.useTransitivityRule = cb.getBoolean('model.rule.transitivity', false);

         sqPotentials = true;
		}
	}

	public DistBibERDBLPACM(ConfigBundle cb) {
		log = LoggerFactory.getLogger(this.class);
		config = new PSLConfig(cb);
		ds = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, Paths.get(config.dbPath, ID).toString(), true), cb);
		model = new PSLModel(this, ds);
	}

	/**
	 * Defines the logical predicates used in this model
	 */
	private void definePredicates() {
        model.add predicate: "Block", types: [ConstantType.UniqueID, ConstantType.UniqueID];
        model.add predicate: "DB", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "Similar", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "Same", types: [ConstantType.UniqueID, ConstantType.UniqueID];
	}

	/**
	 * Defines the rules for this model, optionally including transitivty and
	 * symmetry based on the PSLConfig options specified
	 */
	private void defineRules() {
		log.info("Defining model rules");
		model.add(
			rule: ( Similar(P1,P2) & Block(P1,B) & Block(P2,B) & DB(P1,D1) & DB(P2,D2) & (D1-D2) & (P1-P2) ) >> Same(P1,P2),
			squared: config.sqPotentials,
			weight : config.weightMap["Similar"]
		);

		if (config.useTransitivityRule) {
            model.add(
                rule: ( Same(P1,P2) & Same(P2,P3) & (P1-P3) & Block(P1,B) & Block(P2,B) & Block(P3,B)) >> Same(P1,P3),
                squared: config.sqPotentials,
                weight : config.weightMap["Transitivity"]
            );
		}

		model.add(
			rule: ( DB(P1,D1) & DB(P2,D2) & (D1-D2) & Block(P1,B) & Block(P2,B)) >> ~Same(P1,P2),
			squared:config.sqPotentials,
			weight: config.weightMap["Prior"]
		);

		log.debug("model: {}", model);
	}

	/**
	 * Load data from text files into the DataStore. Three partitions are defined
	 * and populated: observations, targets, and truth.
	 * Observations contains evidence that we treat as background knowledge and
	 * use to condition our inferences
	 * Targets contains the inference targets - the unknown variables we wish to infer
	 * Truth contains the true values of the inference variables and will be used
	 * to evaluate the model's performance
	 */
	private void loadData(Partition obsPartition, Partition targetsPartition, Partition truthPartition) {
		log.info("Loading data into database");

		Inserter inserter = ds.getInserter(Similar, obsPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, config.simPath).toString());

        if (!config.distributed) {
		    inserter = ds.getInserter(Block, obsPartition);
    		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "Blocks.txt").toString());
        }

		inserter = ds.getInserter(DB, obsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "DB.txt").toString());

		inserter = ds.getInserter(Same, targetsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, config.targetsPath).toString());

		inserter = ds.getInserter(Same, truthPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, config.truthPath).toString());

	}

	/**
	 * Run inference to infer the same publications between two datasets
	 */
	private void runInference(Partition obsPartition, Partition targetsPartition) {
		log.info("Starting inference");

		Date infStart = new Date();
		HashSet closed = new HashSet<StandardPredicate>([Block, DB, Similar]);

      if (!config.distributed) {
         Database inferDB = ds.getDatabase(targetsPartition, closed, obsPartition);
         MPEInference mpe = new MPEInference(model, inferDB, config.cb);
         mpe.mpeInference();
         mpe.close();
         inferDB.close();
      } else {
         if (config.master) {
            Database inferDB = ds.getDatabase(targetsPartition, closed, obsPartition);
            DistributedMPEInferenceMaster mpe = new DistributedMPEInferenceMaster(model, inferDB, config.cb);
            mpe.mpeInference(obsPartition.getName(), Block.getName(), computeParitions(computeBlocks()));
            mpe.close();
            inferDB.close();
         } else {
            DistributedMPEInferenceWorker mpe = new DistributedMPEInferenceWorker(model, ds, config.cb, targetsPartition, closed, obsPartition);
            mpe.listen();
            mpe.close();
         }
      }

		log.info("Finished inference in {}", TimeCategory.minus(new Date(), infStart));
	}

	/**
	 * Writes the output of the model into a file
	 */
	private void writeOutput(Partition targetsPartition) {
		Database resultsDB = ds.getDatabase(targetsPartition);
		PrintStream ps = new PrintStream(new File(Paths.get(config.outputPath, "same_infer-"+config.runid+".txt").toString()));
		AtomPrintStream aps = new DefaultAtomPrintStream(ps);
		Set atomSet = Queries.getAllAtoms(resultsDB, Same);
		for (Atom a : atomSet) {
			aps.printAtom(a);
		}

		aps.close();
		ps.close();
		resultsDB.close();
	}

	/**
	 * Run statistical evaluation scripts to determine the quality of the inferences
	 * relative to the defined truth.
	 */
	private void evalResults(Partition targetsPartition, Partition truthPartition) {
		Database resultsDB = ds.getDatabase(targetsPartition, [Same] as Set);
		Database truthDB = ds.getDatabase(truthPartition, [Same] as Set);
		DiscretePredictionComparator dpc = new DiscretePredictionComparator(resultsDB);
		ContinuousPredictionComparator cpc = new ContinuousPredictionComparator(resultsDB);
		dpc.setBaseline(truthDB);
		//	 dpc.setThreshold(0.99);
		cpc.setBaseline(truthDB);
		DiscretePredictionStatistics stats = dpc.compare(Same);
		double mse = cpc.compare(Same);
		log.info("MSE: {}", mse);
		log.info("Accuracy {}, Error {}",stats.getAccuracy(), stats.getError());
		log.info(
				"Positive Class: precision {}, recall {}",
				stats.getPrecision(DiscretePredictionStatistics.BinaryClass.POSITIVE),
				stats.getRecall(DiscretePredictionStatistics.BinaryClass.POSITIVE));
		log.info("Negative Class Stats: precision {}, recall {}",
				stats.getPrecision(DiscretePredictionStatistics.BinaryClass.NEGATIVE),
				stats.getRecall(DiscretePredictionStatistics.BinaryClass.NEGATIVE));

		resultsDB.close();
		truthDB.close();
	}

   /**
    * Take the blocks and assign them to workers.
    */
   // TODO(eriq): This is super messy.
   private String[][][] computeParitions(Map<String, List<String>> blocks) {
      // [worker][row][col]
      List<List<String[]>> partitions = new ArrayList<List<String[]>>();
      for (int i = 0; i < config.cb.getList('distributedmpeinference.workers', null).size(); i++) {
         partitions.add(new ArrayList<String[]>());
      }

      // Transform blocks into tranmission format: [block][row][col]
      List<List<String[]>> blockRows = new ArrayList<String[][]>();
      for (Map.Entry<String, List<String>> block : blocks.entrySet()) {
         List<String[]> rows = new ArrayList<String[]>();

         for (int i = 0; i < block.getValue().size(); i++) {
            // Note the groovy syntax.
            String[] row = [block.getValue().get(i), block.getKey()];
            rows.add(row);
         }

         blockRows.add(rows);
      }

      // TODO(eriq): Better, even greedy is better.
      // Just assign round robin.
      for (int i = 0; i < blockRows.size(); i++) {
         partitions.get(i % partitions.size()).addAll(blockRows.get(i));
      }

      // Transform for tranmission.
      String[][][] transmissionPartitions = new String[partitions.size()][][];
      for (int i = 0; i < partitions.size(); i++) {
         transmissionPartitions[i] = new String[partitions.get(i).size()][];
         for (int j = 0; j < partitions.get(i).size(); j++) {
            transmissionPartitions[i][j] = partitions.get(i).get(j);
         }
      }

      return transmissionPartitions;
   }

   // Just block by location.
   private Map<String, List<String>> computeBlocks() {
      List<String> lines = Files.readAllLines(Paths.get(config.dataPath, "Blocks.txt"), Charset.defaultCharset());

      Map<String, List<String>> blocks = new HashMap<String, List<String>>();
      for (String line : lines) {
         String[] parts = line.split("\t");

         if (!blocks.containsKey(parts[1])) {
            blocks.put(parts[1], new ArrayList<String>());
         }

         blocks.get(parts[1]).add(parts[0]);
      }

      return blocks;
   }

	public void run() {
		log.info("Running experiment...");

		Partition obsPartition = ds.getPartition(PARTITION_OBSERVATIONS);
		Partition targetsPartition = ds.getPartition(PARTITION_TARGETS);
		Partition truthPartition = ds.getPartition(PARTITION_TRUTH);

		definePredicates();
		defineRules();
		loadData(obsPartition, targetsPartition, truthPartition);
		runInference(obsPartition, targetsPartition);

		if (!config.distributed || config.master) {
         writeOutput(targetsPartition);
         evalResults(targetsPartition, truthPartition);
      }

		ds.close();
	}

	/**
	 * Parse the command line options and populate them into a ConfigBundle
	 * Currently the only argument supported is the path to the data directory
	 * @param args - the command line arguments provided during the invocation
	 * @return - a ConfigBundle populated with options from the command line options
	 */
	public static ConfigBundle populateConfigBundle(String[] args) {
		ConfigBundle cb = ConfigManager.getManager().getBundle(ID);

      cb.setProperty('distributed', false);
      cb.setProperty('master', false);
        cb.setProperty('experiment.data.path', args[0]);
        cb.setProperty('experiment.similarity.path', args[1]);
        cb.setProperty('experiment.targets.path', args[2]);
        cb.setProperty('experiment.truth.path', args[3]);
        cb.setProperty('experiment.runid', args[4]);

      for (int i = 5; i < args.length; i++) {
         if (args[i].equals("--worker")) {
            cb.setProperty('distributed', true);
            cb.setProperty('master', false);
         } else if (args[i].equals("--master")) {
            cb.setProperty('distributed', true);
            cb.setProperty('master', true);
         } else {
            throw new RuntimeException("Unknown argument: [" + args[i] + "]");
         }
      }

		// TEST(eriq)
		//cb.addProperty('distributedmpeinference.workers', 'waterdance.soe.ucsc.edu:12345');
		//cb.addProperty('distributedmpeinference.workers', 'slamdance.soe.ucsc.edu:12345');
		//cb.addProperty('distributedmpeinference.workers', 'eriqs-shit.com:12345');

		return cb;
	}

	/**
	 * Run this model from the command line
	 * @param args - the command line arguments
	 */
	public static void main(String[] args) {
		ConfigBundle configBundle = populateConfigBundle(args);
		DistBibERDBLPACM er = new DistBibERDBLPACM(configBundle);
		er.run();
	}
}

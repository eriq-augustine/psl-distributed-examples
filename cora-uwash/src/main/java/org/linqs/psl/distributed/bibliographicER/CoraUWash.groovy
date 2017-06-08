package org.linqs.psl.distributed.bibliographicER;

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
import org.linqs.psl.utils.evaluation.statistics.QuickPredictionComparator;
import org.linqs.psl.utils.evaluation.statistics.QuickPredictionStatistics;

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

public class CoraUWash  {
	private static final String PARTITION_OBSERVATIONS = "observations";
	private static final String PARTITION_TARGETS = "targets";
	private static final String PARTITION_TRUTH = "truth";

   private static final String ID = "corauwash";

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

		public String runid;

		public Map weightMap = [
			"SimilarTitles":40,
			"SimilarNames":40,
			"NotSimilarAuthors":40,
			"SamePubSameAuthor":20,
			"Co-occurrence":20,
			"Transitivity":40,
			"NotSameCoAuthor":40,
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

			runid = cb.getString('runid','0');

			weightMap["SimilarTitles"] = cb.getInteger('model.weights.similarTitles', weightMap["SimilarTitles"]);
			weightMap["SimilarName"] = cb.getInteger('model.weights.similarNames', weightMap["SimilarNames"]);
			weightMap["NotSimilarAuthors"] = cb.getInteger('model.weights.notSimilarAuthors', weightMap["NotSimilarAuthors"]);
			weightMap["SamePubSameAuthor"] = cb.getInteger('model.weights.SamePubSameAuthor', weightMap["SamePubSameAuthor"]);
			weightMap["NotSameCoAuthor"] = cb.getInteger('model.weights.notSameCoAuthor', weightMap["NotSameCoAuthor"]);
			weightMap["Transitivity"] = cb.getInteger('model.weights.transitivity', weightMap["Transitivity"]);
			weightMap["Co-occurrence"] = cb.getInteger('model.weights.cooccurrence', weightMap["Co-occurrence"]);
			weightMap["Prior"] = cb.getInteger('model.weights.prior', weightMap["Prior"]);
			useTransitivityRule = cb.getBoolean('model.rule.transitivity', false);

			sqPotentials = true;
		}

	}

	public CoraUWash(ConfigBundle cb) {
		log = LoggerFactory.getLogger(this.class);
		config = new PSLConfig(cb);
		ds = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, Paths.get(config.dbPath, ID).toString(), true), cb);
		model = new PSLModel(this, ds);
	}

	/**
	 * Defines the logical predicates used in this model
	 */
	private void definePredicates() {
		model.add predicate: "BlocksAuthors", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "BlocksPubs", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "HaveSimilarTitles", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "HaveSimilarAuthors", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "HaveSimilarNames", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "AreCoAuthors", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "HasAuthor", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "SamePub", types: [ConstantType.UniqueID, ConstantType.UniqueID];
		model.add predicate: "SameAuthor", types: [ConstantType.UniqueID, ConstantType.UniqueID];
	}

	/**
	 * Defines the rules for this model, optionally including transitivty and
	 * symmetry based on the PSLConfig options specified
	 */
	private void defineRules() {
		log.info("Defining model rules");
		model.add(
			rule: ( HaveSimilarTitles(P1,P2) & BlocksPubs(P1,B) & BlocksPubs(P2,B) & (P1-P2) ) >> SamePub(P1,P2),
			squared: config.sqPotentials,
			weight : config.weightMap["SimilarTitles"]
		);

		model.add(
			rule: ( HaveSimilarNames(A1,A2) & BlocksAuthors(A1,B) & BlocksAuthors(A2,B) & (A1-A2) ) >> SameAuthor(A1,A2),
			squared: config.sqPotentials,
			weight : config.weightMap["SimilarNames"]
		);

		model.add(
			rule: ( ~HaveSimilarAuthors(P1,P2) & BlocksPubs(P1,B) & BlocksPubs(P2,B) & (P1-P2) ) >> ~SamePub(P1,P2),
			squared: config.sqPotentials,
			weight : config.weightMap["NotSimilarAuthors"]
		);

		model.add(
			rule: ( SamePub(P1,P2) & HasAuthor(P1,A1) & HasAuthor(P2,A2) & HaveSimilarNames(A1,A2) & BlocksPubs(P1,PB) & BlocksPubs(P2,PB) & BlocksAuthors(A1,AB) & BlocksAuthors(A2,AB) & (P1-P2) & (A1-A2)) >> SameAuthor(A1,A2),
			squared: config.sqPotentials,
			weight : config.weightMap["SamePubSameAuthor"]
		);

		model.add(
			rule: ( AreCoAuthors(A1,A2) & AreCoAuthors(A3,A4) & SameAuthor(A1,A3) & HaveSimilarNames(A2,A4) & BlocksAuthors(A1,B) & BlocksAuthors(A2,B) & BlocksAuthors(A3,B) & BlocksAuthors(A4,B) & (A1-A2) & (A3-A4) & (A1-A3) & (A2-A4)) >> SameAuthor(A2,A4),
			squared: config.sqPotentials,
			weight : config.weightMap["Co-occurrence"]
		);

		if (config.useTransitivityRule) {
			model.add(
				rule: ( SamePub(P1,P2) & SamePub(P2,P3) & (P1-P3) & BlocksPubs(P1,B) & BlocksPubs(P2,B) & BlocksPubs(P3,B)) >> SamePub(P1,P3),
				squared: config.sqPotentials,
				weight : config.weightMap["Transitivity"]
			);

			model.add(
				rule: ( SameAuthor(A1,A2) & SameAuthor(A2,A3) & (A1-A3) & BlocksAuthors(A1,B) & BlocksAuthors(A2,B) & BlocksAuthors(A3,B)) >> SameAuthor(A1,A3),
				squared: config.sqPotentials,
				weight : config.weightMap["Transitivity"]
			);
		}

		model.add(
			rule: ( (P1-P2) & BlocksPubs(P1,B) & BlocksPubs(P2,B)) >> ~SamePub(P1,P2),
			squared:config.sqPotentials,
			weight: config.weightMap["Prior"]
		);

		model.add(
			rule: ( (A1-A2) & BlocksAuthors(A1,B) & BlocksAuthors(A2,B)) >> ~SameAuthor(A1,A2),
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

		Inserter inserter = ds.getInserter(HaveSimilarTitles, obsPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "HaveSimilarTitles.txt").toString());

        if (!config.distributed){
    		inserter = ds.getInserter(BlocksAuthors, obsPartition);
	    	InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "BlocksAuthors.txt").toString());
        }

		inserter = ds.getInserter(BlocksPubs, obsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "BlocksPubs.txt").toString());

		inserter = ds.getInserter(HaveSimilarNames, obsPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "HaveSimilarNames.txt").toString());

		inserter = ds.getInserter(HaveSimilarAuthors, obsPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "HaveSimilarAuthors.txt").toString());

		inserter = ds.getInserter(AreCoAuthors, obsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "AreCoAuthors.txt").toString());

		inserter = ds.getInserter(HasAuthor, obsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "HasAuthor.txt").toString());

		inserter = ds.getInserter(SamePub, targetsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "SamePub.target.txt").toString());

		inserter = ds.getInserter(SamePub, truthPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "SamePub.truth.txt").toString());

		inserter = ds.getInserter(SameAuthor, targetsPartition);
		InserterUtils.loadDelimitedData(inserter, Paths.get(config.dataPath, "SameAuthor.target.txt").toString());

		inserter = ds.getInserter(SameAuthor, truthPartition);
		InserterUtils.loadDelimitedDataTruth(inserter, Paths.get(config.dataPath, "SameAuthor.truth.txt").toString());

	}

	/**
	 * Run inference to infer the unknown Knows relationships between people.
	 */
	private void runInference(Partition obsPartition, Partition targetsPartition) {
		log.info("Starting inference");

		Date infStart = new Date();
		HashSet closed = new HashSet<StandardPredicate>([BlocksAuthors, BlocksPubs, HaveSimilarTitles, HaveSimilarAuthors, HaveSimilarNames, AreCoAuthors, HasAuthor]);

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
            mpe.mpeInference(obsPartition.getName(), BlocksAuthors.getName(), computeParitions(computeBlocks()));
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
		Set atomSet = Queries.getAllAtoms(resultsDB,SamePub);
		for (Atom a : atomSet) {
			aps.printAtom(a);
		}
		atomSet = Queries.getAllAtoms(resultsDB,SameAuthor);
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
		Database resultsDB = ds.getDatabase(targetsPartition, [SamePub,SameAuthor] as Set);
		Database truthDB = ds.getDatabase(truthPartition, [SamePub,SameAuthor] as Set);

		QuickPredictionComparator qpc = new QuickPredictionComparator(resultsDB);
		qpc.setBaseline(truthDB);

        //Compare for author
		QuickPredictionStatistics stats = qpc.compare(SameAuthor);
        log.info("Stats for Author");
		log.info("MSE: {}", stats.getContinuousMetricScore());
		log.info("Accuracy {}, Error {}",stats.getAccuracy(), stats.getError());
		log.info(
				"Positive Class: precision {}, recall {}",
				stats.getPrecision(QuickPredictionStatistics.BinaryClass.POSITIVE),
				stats.getRecall(QuickPredictionStatistics.BinaryClass.POSITIVE));
		log.info("Negative Class Stats: precision {}, recall {}",
				stats.getPrecision(QuickPredictionStatistics.BinaryClass.NEGATIVE),
				stats.getRecall(QuickPredictionStatistics.BinaryClass.NEGATIVE));

        //Compare for publications
		stats = qpc.compare(SamePub);
        log.info("Stats for Publications");
		log.info("MSE: {}", stats.getContinuousMetricScore());
		log.info("Accuracy {}, Error {}",stats.getAccuracy(), stats.getError());
		log.info(
				"Positive Class: precision {}, recall {}",
				stats.getPrecision(QuickPredictionStatistics.BinaryClass.POSITIVE),
				stats.getRecall(QuickPredictionStatistics.BinaryClass.POSITIVE));
		log.info("Negative Class Stats: precision {}, recall {}",
				stats.getPrecision(QuickPredictionStatistics.BinaryClass.NEGATIVE),
				stats.getRecall(QuickPredictionStatistics.BinaryClass.NEGATIVE));

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
      List<String> lines = Files.readAllLines(Paths.get(config.dataPath, "BlocksAuthors.txt"), Charset.defaultCharset());

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

		int argsOffset = 0;
		if (args[0].equals("--worker")) {
			cb.setProperty('distributed', true);
			cb.setProperty('master', false);
			argsOffset = 1;
		} else if (args[0].equals("--master")) {
			cb.setProperty('distributed', true);
			cb.setProperty('master', true);
			argsOffset = 1;
		} else if (args[0].startsWith("--runid=")) {
			cb.setProperty('distributed', false);
			cb.setProperty('master', false);
		}
		else {
			throw new RuntimeException("Unknown argument: [" + args[0] + "]");
		}

		cb.setProperty('experiment.runid', args[argsOffset + 0].substring("--runid=".length()));

		return cb;
	}

	/**
	 * Run this model from the command line
	 * @param args - the command line arguments
	 */
	public static void main(String[] args) {
		ConfigBundle configBundle = populateConfigBundle(args);
		CoraUWash er = new CoraUWash(configBundle);
		er.run();
	}
}

require_relative 'computeNumberGroundins'
require_relative 'genData'

require 'fileutils'
require 'open3'

START_PEOPLE = 300
END_PEOPLE = 600
PEOPLE_INCREMENT = 100

START_LOCATIONS = 10
END_LOCATIONS = 30
LOCATIONS_INCREMENT = 10

RESULTS_DIR = 'results'
OUT_FILE = 'out.txt'
ERR_FILE = 'out.err'
BASH_BIN_PATH = '/bin/bash'

RUN_DATA_ONLY = 'dataonly'
RUN_STANDALONE = 'standalone'
RUN_WORKER = 'worker'
RUN_MASTER = 'master'

INFER_OUTPUT_PATH = File.join('.', 'output', 'friends_infer.txt')

# TEST
# MASTER_SLEEP_TIME_SEC = 60
MASTER_SLEEP_TIME_SEC = 5

def ensureData(people, locations)
   return GenData.main(['--people', "#{people}", '--locations', "#{locations}"])
end

def runBase(args, dataPath, runType)
   outDir = File.join(RESULTS_DIR, "#{File.basename(dataPath)}_#{runType}")

   if (File.exists?(outDir))
      puts "Results directory for [#{outDir}] already exist, skipping run."
      return
   end

   FileUtils.mkdir_p(outDir)

   command = "#{BASH_BIN_PATH} #{args.map{|arg| "'#{arg}'"}.join(' ')}"
   outPath = File.join(outDir, OUT_FILE)
   errPath = File.join(outDir, ERR_FILE)

   run(command, outPath, errPath)

   return outDir
end

def runStandalone(people, locations, dataPath)
   args = [
      'run-standalone.sh',
      '--data', dataPath
   ]

   runBase(args, dataPath, RUN_STANDALONE)
end

def runWorker(people, locations, dataPath, numWorkers)
   args = [
      'run-worker.sh',
      '--data', dataPath
   ]

   runBase(args, dataPath, "#{RUN_WORKER}_#{numWorkers}")
end

def runMaster(people, locations, dataPath, workers)
   args = [
      'run-master.sh',
      '--data', dataPath
   ]
   args += workers

   # The master needs to sleep a bit to make sure that the workers are ready.
   sleep(MASTER_SLEEP_TIME_SEC)

   outDir = runBase(args, dataPath, RUN_MASTER)

   # Move the output inference.
   FileUtils.mv(INFER_OUTPUT_PATH, outDir)
end

def run(command, outFile=nil, errFile=nil)
   # TEST
   puts "Running command: #{command}"

   stdout, stderr, status = Open3.capture3(command)

   if (outFile != nil)
      File.open(outFile, 'w'){|file|
         file.puts(stdout)
      }
   end

   if (errFile != nil)
      File.open(errFile, 'w'){|file|
         file.puts(stderr)
      }
   end

   success = true
   if (status.exitstatus() != 0)
      $stderr.puts("Failed to run command: [#{command}]. Exited with status: #{status}")
      success = false
   end

   return success
end

def loadArgs(args)
   if (args.size() < 1 || (args.map{|arg| arg.gsub('-', '').downcase()} & ['help', 'h']).any?())
      puts "USAGE: ruby #{$0} <--dataOnly | --standalone>"
      puts "       ruby #{$0}  --master <worker address> ..."
      puts "       ruby #{$0}  --worker <num workers>"
      puts "Worker addresses only need to be provided for a master instance."
      exit(1)
   end

   runType = nil
   workers = []
   numWorkers = 0

   if (args.size() > 0)
      flag = args.shift()

      if (flag == '--dataOnly')
         runType = RUN_DATA_ONLY
      elsif (flag == '--standalone')
         runType = RUN_STANDALONE
      elsif (flag == '--worker')
         runType = RUN_WORKER
      elsif (flag == '--master')
         runType = RUN_MASTER
      else
         puts "Unknown flag: #{flag}"
         exit(2)
      end
   end

   if (runType == RUN_MASTER)
      workers = args
      numWorkers = workers.size()

      if (workers.size() == 0)
         puts "Need workers if you run in master mode."
         exit(3)
      end
   elsif (runType == RUN_WORKER)
      if (args.size() != 1)
         puts "Need number of workers if you run in worker mode."
         exit(4)
      end

      numWorkers = args.shift().to_i()
   end

   return runType, workers, numWorkers
end

def main(runType, workers, numWorkers)
   # Go through each run by approximate number of grounds.
   # [[people, locations, groundings], ...]
   runs = []

   people = START_PEOPLE
   while (people <= END_PEOPLE)
      locations = START_LOCATIONS
      while (locations <= END_LOCATIONS)
         runs << [people, locations, ComputeNumberGroundins.numberGroundings(people, locations)]

         locations += LOCATIONS_INCREMENT
      end

      people += PEOPLE_INCREMENT
   end

   runs.sort!{|a, b| a[2] <=> b[2]}

   runs.each{|people, locations, groundings|
      puts "Starting Run -- People: #{people}, Locations: #{locations}, Num Groundings: #{groundings}"

      dataPath = ensureData(people, locations)

      if (runType == RUN_DATA_ONLY)
         next
      elsif (runType == RUN_STANDALONE)
         runStandalone(people, locations, dataPath)
      elsif (runType == RUN_WORKER)
         runWorker(people, locations, dataPath, numWorkers)
      elsif (runType == RUN_MASTER)
         runMaster(people, locations, dataPath, workers)
      end
   }
end

if ($0 == __FILE__)
   main(*loadArgs(ARGV))
end

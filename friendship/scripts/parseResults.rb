# Parse the reuslts for the stats!

require 'pp'

TARGET_FILE = 'out.txt'
RESULTS_DIR = 'results'
BYTES_PER_MEGABYTE = 1024 * 1024

WORKER_INDEXES = {
   'pampelonne' => 0,
   'pebble' => 1,
   'ibi' => 2,
   'kish' => 3
}

# TODO(eriq): Missing local vars.
def parseStandalone(path)
   workerStats = {}
   masterStats = {}

   inferenceStartTimeMS = nil
   groundingStartTimeMS = nil
   termGenStartTimeMS = nil

   File.open(path, 'r'){|file|
      file.each{|line|
         line.strip()

         if (match = line.match(/Stats -- Memory \(Bytes\): (\d+)/))
            masterStats['memory'] = match[1].to_i() / BYTES_PER_MEGABYTE
            workerStats['memory'] = match[1].to_i() / BYTES_PER_MEGABYTE
         elsif (match = line.match(/^(\d+) .* - Beginning inference\.$/))
            inferenceStartTimeMS = match[1].to_i()
         elsif (match = line.match(/^(\d+) .* - Inference complete. Writing results to Database\.$/))
            inferenceEndTimeMS = match[1].to_i()
            masterStats['inference'] = inferenceEndTimeMS - inferenceStartTimeMS
            workerStats['inference'] = inferenceEndTimeMS - inferenceStartTimeMS
         elsif (match = line.match(/^(\d+) .* - Grounding out model\.$/))
            groundingStartTimeMS = match[1].to_i()
         elsif (match = line.match(/^(\d+) .* - Initializing objective terms for/))
            groundingEndTimeMS = match[1].to_i()
            termGenStartTimeMS = match[1].to_i()
            workerStats['grounding'] = groundingEndTimeMS - groundingStartTimeMS
         elsif (match = line.match(/^(\d+) .* - Generated (\d+) objective terms from /))
            termGenEndTimeMS = match[1].to_i()
            workerStats['termGen'] = termGenEndTimeMS - termGenStartTimeMS
            workerStats["terms"] = match[2].to_i()
         elsif (match = line.match(/^(\d+) .* - Performing optimization with (\d+) variables and /))
            workerStats['globalVars'] = match[2].to_i()
         end
      }
   }

   workerStats['localVars'] = -1

   return masterStats, workerStats
end

def parseWorker(path)
   stats = {}

   inferenceStartTimeMS = nil
   groundingStartTimeMS = nil
   termGenStartTimeMS = nil

   File.open(path, 'r'){|file|
      file.each{|line|
         line.strip()

         if (match = line.match(/Stats -- Memory \(Bytes\): (\d+), Terms: (\d+), Global Variables: (\d+) Local Variables: (\d+)/))
            stats["memory"] = match[1].to_i() / BYTES_PER_MEGABYTE
            stats["terms"] = match[2].to_i()
            stats['globalVars'] = match[3].to_i()
            stats['localVars'] = match[4].to_i()
         elsif (match = line.match(/^(\d+) .* - Beginning inference\.$/))
            inferenceStartTimeMS = match[1].to_i()
         elsif (match = line.match(/^(\d+) .* - Inference complete. Writing results to Database\.$/))
            inferenceEndTimeMS = match[1].to_i()
            stats['inference'] = inferenceEndTimeMS - inferenceStartTimeMS
         elsif (match = line.match(/^(\d+) .* - Grounding out model\.$/))
            groundingStartTimeMS = match[1].to_i()
         elsif (match = line.match(/^(\d+) .* - Initializing objective terms for/))
            groundingEndTimeMS = match[1].to_i()
            termGenStartTimeMS = match[1].to_i()
            stats['grounding'] = groundingEndTimeMS - groundingStartTimeMS
         elsif (match = line.match(/^(\d+) .* objective terms from /))
            termGenEndTimeMS = match[1].to_i()
            stats['termGen'] = termGenEndTimeMS - termGenStartTimeMS
         end
      }
   }

   return stats
end

def parseMaster(path)
   stats = {}

   inferenceStartTimeMS = nil

   File.open(path, 'r'){|file|
      file.each{|line|
         line.strip()

         if (match = line.match(/Stats -- Memory \(Bytes\): (\d+)/))
            stats['memory'] = match[1].to_i() / BYTES_PER_MEGABYTE
         elsif (match = line.match(/^(\d+) .* - Beginning inference\.$/))
            inferenceStartTimeMS = match[1].to_i()
         elsif (match = line.match(/^(\d+) .* - Inference complete. Writing results to Database\.$/))
            inferenceEndTimeMS = match[1].to_i()
            stats['inference'] = inferenceEndTimeMS - inferenceStartTimeMS
         end
      }
   }

   return stats
end

def parse()
   runs = Hash.new{|hash, key| hash[key] = {:workers => Array.new(WORKER_INDEXES.size())}}

   Dir.glob("#{RESULTS_DIR}/**/#{TARGET_FILE}").each{|path|
      if (match = File.dirname(path).match(/base_(\d+)_(\d+)_(standalone)_([a-z]+)/))
         people, locations, purpose, host = match.captures()
         workers = 1
      elsif (match = File.dirname(path).match(/base_(\d+)_(\d+)_([a-z]+)_(\d)_([a-z]+)/))
         people, locations, purpose, workers, host = match.captures()
      else
         next
      end

      runId = "#{workers}_#{people}_#{locations}"
      if (!(['master', 'worker', 'standalone'].include?(purpose)))
         puts "ERROR: Unknown purpose: #{purpose}"
         next
      end

      if (purpose == 'master')
         runs[runId][:master] = parseMaster(path)
      elsif (purpose == 'worker')
         runs[runId][:workers][WORKER_INDEXES[host]] = parseWorker(path)
      elsif (purpose == 'standalone')
         masterStats, workerStats = parseStandalone(path)
         runs[runId][:master] = masterStats
         runs[runId][:workers][0] = workerStats
      end
   }

   return runs
end

def formatWorkerRun(key, run)
   workerCount, people, locations = key.split('_').map{|value| value.to_i()}

   output = [workerCount, people, locations]

   meanStats = run[:workersMean]
   output += [
      meanStats['grounding'],
      meanStats['termGen'],
      meanStats['terms'],
      meanStats['localVars'],
      meanStats['globalVars'],
      meanStats['memory'],
   ]

   for i in 0...workerCount
      worker = run[:workers][i]

      output += [
         worker['grounding'],
         worker['termGen'],
         worker['terms'],
         worker['localVars'],
         worker['globalVars'],
         worker['memory'],
      ]
   end

   return output.join("\t")
end

def printMasterRuns(runs)
   # Masters get merged accross number of workers.
   # 4 different experiments.
   # {runKey => {statKey => [4], ...}, ...}
   mergedRuns = Hash.new{|runHash, runKey| runHash[runKey] = Hash.new{|statHash, statKey| statHash[statKey] = Array.new(4, -1)}}

   runs.each_pair{|key, run|
      workerCount, people, locations = key.split('_').map{|value| value.to_i()}

      if (!run.has_key?(:master))
         next
      end

      mergedKey = "#{people}_#{locations}"
      mergedRuns[mergedKey]['inference'][workerCount - 1] = run[:master]['inference']
      mergedRuns[mergedKey]['memory'][workerCount - 1] = run[:master]['memory']

      mergedRuns[mergedKey]['grounding'][workerCount - 1] = run[:workersMean]['grounding']
      mergedRuns[mergedKey]['termGen'][workerCount - 1] = run[:workersMean]['termGen']

      # Inference + Grounding + Term Generation
      mergedRuns[mergedKey]['computation'][workerCount - 1] = run[:master]['inference'] + run[:workersMean]['grounding'] + run[:workersMean]['termGen']
   }

   rows = []
   mergedRuns.each_pair{|key, mergedRun|
      people, locations = key.split('_').map{|value| value.to_i()}

      output = [people, locations]
      output += mergedRun['grounding']
      output += mergedRun['termGen']
      output += mergedRun['inference']
      output += mergedRun['computation']
      output += mergedRun['memory']

      rows << output.join("\t")
   }

   puts rows.sort().join("\n")
end

def print(runs)
   puts "Masters"
   printMasterRuns(runs)

   puts "Workers"
   puts runs.to_a().map{|key, run| formatWorkerRun(key, run)}.sort().join("\n")
end

def main(args)
   runs = parse()

   # Calc worker aggregates.
   runs.each_pair{|key, run|
      workerCount, people, locations = key.split('_').map{|value| value.to_i()}

      means = Hash.new{|hash, key| hash[key] = 0}

      for i in 0...workerCount
         worker = run[:workers][i]
         worker.each_pair{|key, value|
            means[key] += value
         }
      end

      means.each_key{|key|
         means[key] /= workerCount
      }

      run[:workersMean] = means
   }

   print(runs)
end

if ($0 == __FILE__)
   main(ARGV)
end

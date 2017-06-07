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
            stats['Memory'] = match[1].to_i() / BYTES_PER_MEGABYTE
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
      match = File.dirname(path).match(/base_(\d+)_(\d+)_([a-z]+)_(\d)_([a-z]+)/)
      if (!match)
         next
      end

      people, locations, purpose, workers, host = match.captures()
      runId = "#{workers}_#{people}_#{locations}"

      if (!(['master', 'worker'].include?(purpose)))
         puts "ERROR: Unknown purpose: #{purpose}"
         next
      end

      if (purpose == 'master')
         runs[runId][:master] = parseMaster(path)
      elsif (purpose == 'worker')
         runs[runId][:workers][WORKER_INDEXES[host]] = parseWorker(path)
      end
   }

   return runs
end

def formatMasterRun(key, run)
   workerCount, people, locations = key.split('_').map{|value| value.to_i()}

   output = [people, locations]
   output << run[:master]['inference']
   output << run[:master]['memory']

   return output.join("\t")
end

def formatWorkerRun(key, run)
   workerCount, people, locations = key.split('_').map{|value| value.to_i()}

   output = [people, locations]

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

def print(runs)
   puts "Masters"
   puts runs.to_a().map{|key, run| formatMasterRun(key, run)}.sort().join("\n")

   puts "Workers"
   puts runs.to_a().map{|key, run| formatWorkerRun(key, run)}.sort().join("\n")
end

def main(args)
   runs = parse()

   # Calc worker aggregates.
   runs.each_value{|run|
      means = Hash.new{|hash, key| hash[key] = 0}

      run[:workers].each{|worker|
         worker.each_pair{|key, value|
            means[key] += value
         }
      }

      means.each_key{|key|
         means[key] /= run[:workers].size()
      }

      run[:workersMean] = means
   }

   print(runs)
end

if ($0 == __FILE__)
   main(ARGV)
end

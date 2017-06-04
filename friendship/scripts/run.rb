require_relative 'computeNumberGroundins'
require_relative 'genData'

START_PEOPLE = 75
END_PEOPLE = 750
PEOPLE_INCREMENT = 75

START_LOCATIONS = 10
END_LOCATIONS = 30
LOCATIONS_INCREMENT = 5

RESULTS_DIR = 'results'
OUT_FILE = 'out.txt'
ERR_FILE = 'out.err'
BASH_BIN_PATH = '/bin/bash'

def ensureData(people, locations)
   return GenData.main(['--people', "#{people}", '--locations', "#{locations}"])
end

def runStandalone(people, locations, dataPath)
   args = [
      'run-standalone.sh',
      '--data', dataPath
   ]

   command = "#{BASH_BIN_PATH} #{args.map{|arg| "'#{arg}'"}.join(' ')}"
   outPath = File.join(RESULTS_DIR, OUT_FILE)
   errPath = File.join(RESULTS_DIR, ERR_FILE)

   run(command, outPath, errPath)
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
   if (args.size() > 1 || (args.map{|arg| arg.gsub('-', '').downcase()} & ['help', 'h']).any?())
      puts "USAGE: ruby #{$0} [--dataOnly]"
      exit(1)
   end

   dataOnly = false
   if (args.size() > 0)
      flag = args.shift()
      if (flag != '--dataOnly')
         puts "Unknown flag: #{flag}"
         exit(2)
      end

      dataOnly = true
   end

   return dataOnly
end

def main(dataOnly)
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

      dataDir = ensureData(people, locations)

      # TEST
      puts dataDir

      if (dataOnly)
         next
      end
   }
end

if ($0 == __FILE__)
   main(*loadArgs(ARGV))
end

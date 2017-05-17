DATA_DIR = File.join('..', 'data')
SIMILAR_OBS_PATH = File.join(DATA_DIR, 'similar_obs.txt')
LOCATION_OBS_PATH = File.join(DATA_DIR, 'location_obs.txt')
FRIENDS_OBS_PATH = File.join(DATA_DIR, 'friends_obs.txt')
FRIENDS_TARGET_PATH = File.join(DATA_DIR, 'friends_targets.txt')
FRIENDS_TRUTH_PATH = File.join(DATA_DIR, 'friends_truth.txt')

LOC_SC = 'Santa Cruz'
LOC_LA = 'Los Angeles'
LOC_NY = 'New York'

LOCATIONS = [
   LOC_SC,
   LOC_LA,
   LOC_NY,
]

PROXIMITY = {
   LOC_SC => {
      LOC_SC => 1.0,
      LOC_LA => 0.7,
      LOC_NY => 0.1,
   },
   LOC_LA => {
      LOC_SC => 0.7,
      LOC_LA => 1.0,
      LOC_NY => 0.2,
   },
   LOC_NY => {
      LOC_SC => 0.1,
      LOC_LA => 0.2,
      LOC_NY => 1.0,
   },
}

def writeData(locations, similarity, friendship)
   File.open(LOCATION_OBS_PATH, 'w'){|file|
      file.puts(locations.each_with_index().map{|loc, index| "#{index}\t#{loc}"}.join("\n"))
   }

   File.open(SIMILAR_OBS_PATH, 'w'){|file|
      file.puts(similarity.map{|entry| entry.join("\t")}.join("\n"))
   }

   File.open(FRIENDS_TRUTH_PATH, 'w'){|file|
      file.puts(friendship.map{|entry| entry.join("\t")}.join("\n"))
   }

   File.open(FRIENDS_TARGET_PATH, 'w'){|file|
      file.puts(friendship.map{|entry| entry[0...2].join("\t")}.join("\n"))
   }
end

def genData(numPeople)
   random = Random.new(4)

   locations = []
   for i in 0...numPeople
      locations << LOCATIONS.sample(random: random)
   end

   similarity = []
   for i in 0...numPeople
      for j in 0...numPeople
         if i == j
            sim = 1.0
         else
            sim = random.rand()
         end

         similarity << [i, j, sim]
      end
   end

   friendship = []
   for i in 0...numPeople
      for j in 0...numPeople
         if i == j
            friends = 1
         else
            # Chose with proximity bias.
            if (random.rand() <= PROXIMITY[locations[i]][locations[j]])
               friends = 1
            else
               friends = 0
            end
         end

         friendship << [i, j, friends]
      end
   end

   writeData(locations, similarity, friendship)
end

def loadArgs(args)
   if (args.size() != 1 || args.map{|arg| arg.gsub('-', '').downcase()}.include?('help'))
      puts "USAGE: ruby #{$0} <num people>"
      exit(1)
   end

   numPeople = args.shift().to_i()

   return numPeople
end

def main(args)
   genData(*loadArgs(args))
end

if ($0 == __FILE__)
   main(ARGV)
end

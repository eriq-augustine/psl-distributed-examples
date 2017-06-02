# Self pairings are disallowed.

DATA_DIR = File.join('.', 'data')
SIMILAR_OBS_PATH = File.join(DATA_DIR, 'similar_obs.txt')
LOCATION_OBS_PATH = File.join(DATA_DIR, 'location_obs.txt')
FRIENDS_OBS_PATH = File.join(DATA_DIR, 'friends_obs.txt')
FRIENDS_TARGET_PATH = File.join(DATA_DIR, 'friends_targets.txt')
FRIENDS_TRUTH_PATH = File.join(DATA_DIR, 'friends_truth.txt')

SEED = 4

OPTIONS = [
   {
      :id => 'people',
      :short => 'p',
      :long => 'people',
      :valueDesc => 'number of people',
      :desc => 'The number of people to create.',
      :domain => [1, 1000000],
      :default => 10
   },
   {
      :id => 'locations',
      :short => 'l',
      :long => 'locations',
      :valueDesc => 'number of locations',
      :desc => 'The number of locations to create.',
      :domain => [1, 1000000],
      :default => 3
   },
   {
      :id => 'friendshipHigh',
      :short => 'fh',
      :long => 'friendship-high',
      :valueDesc => 'probability',
      :desc => 'The probability that two people in the same location are friends',
      :domain => [0.0, 1.0],
      :default => 1.0
   },
   {
      :id => 'friendshipLow',
      :short => 'fl',
      :long => 'friendship-low',
      :valueDesc => 'probability',
      :desc => 'The probability that two people in different locations are friends',
      :domain => [0.0, 1.0],
      :default => 0.0
   },
   {
      :id => 'similarityMeanHigh',
      :short => 'smh',
      :long => 'similarity-mean-high',
      :valueDesc => 'value',
      :desc => 'The mean of the gaussian distribution to draw high probabilities from.',
      :domain => [0.0, 1.0],
      :default => 0.8
   },
   {
      :id => 'similarityVarianceHigh',
      :short => 'svh',
      :long => 'similarity-variance-high',
      :valueDesc => 'value',
      :desc => 'The variance of the gaussian distribution to draw high probabilities from.',
      :domain => [0.0, 1.0],
      :default => 0.1
   },
   {
      :id => 'similarityMeanLow',
      :short => 'slh',
      :long => 'similarity-mean-low',
      :valueDesc => 'value',
      :desc => 'The mean of the gaussian distribution to draw low probabilities from.',
      :domain => [0.0, 1.0],
      :default => 0.2
   },
   {
      :id => 'similarityVarianceLow',
      :short => 'svl',
      :long => 'similarity-variance-low',
      :valueDesc => 'value',
      :desc => 'The variance of the gaussian distribution to draw low probabilities from.',
      :domain => [0.0, 1.0],
      :default => 0.1
   },
]

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
      # Make sure to add on the zero for the initial value.
      file.puts(friendship.map{|entry| (entry[0...2].push(0)).join("\t")}.join("\n"))
   }
end

def genData(options)
   random = Random.new(SEED)
   numPeople = options['people']

   locations = []
   for i in 0...numPeople
      locations << random.rand(options['locations'])
   end

   friendship = []
   for i in 0...numPeople
      for j in 0...numPeople
         if (i == j)
            next
         else
            friendshipChance = options['friendshipHigh']
            if (locations[i] != locations[j])
               friendshipChance = options['friendshipLow']
            end
            
            if (random.rand(1.0) < friendshipChance)
               friends = 1
            else
               friends = 0
            end
         end

         friendship << [i, j, friends]
      end
   end

   similarity = []
   for i in 0...numPeople
      for j in 0...numPeople
         if (i == j)
            next
         else
            mean = options['similarityMeanHigh']
            variance = options['similarityVarianceHigh']
            if (locations[i] != locations[j])
               mean = options['similarityMeanLow']
               variance = options['similarityVarianceLow']
            end

            sim = gaussian(mean, variance, random)
         end

         sim = [1.0, [0, sim].max()].min()

         similarity << [i, j, sim]
      end
   end

   writeData(locations, similarity, friendship)
end

# Box-Muller: http://www.taygeta.com/random/gaussian.html
def gaussian(mean, variance, rng)
   w = 2

   while (w >= 1.0)
      x1 = 2.0 * rng.rand() - 1
      x2 = 2.0 * rng.rand() - 1
      w = x1 ** 2 + x2 ** 2
   end
   w = Math.sqrt((-2.0 * Math.log(w)) / w)

   return x1 * w * Math.sqrt(variance) + mean
end

def loadArgs(args)
   if ((args.map{|arg| arg.gsub('-', '').downcase()} & ['help', 'h']).any?())
      puts "USAGE: ruby #{$0} [OPTIONS]"
      puts "Options:"

      optionsStr = OPTIONS.map{|option|
         "   -#{option[:short]}, --#{option[:long]} <#{option[:valueDesc]}> - Default: #{option[:default]}. Domain: #{option[:domain]}. #{option[:desc]}"
      }.join("\n")
      puts optionsStr
      exit(1)
   end

   optionValues = OPTIONS.map{|option| [option[:id], option[:default]]}.to_h()

   while (args.size() > 0)
      rawFlag = args.shift()
      flag = rawFlag.strip().sub(/^-+/, '')
      currentOption = nil

      OPTIONS.each{|option|
         if ([option[:short], option[:long]].include?(flag))
            currentOption = option
            break
         end
      }

      if (currentOption == nil)
         puts "Unknown option: #{rawFlag}"
         exit(2)
      end

      if (args.size() == 0)
         puts "Expecting value to argument (#{rawFlag}), but found nothing."
         exit(3)
      end

      value = args.shift().to_f()
      if (currentOption[:default].is_a?(Integer))
         value = value.to_i()
      end

      if (value < currentOption[:domain][0] || value > currentOption[:domain][1])
         puts "Value for #{rawFlag} (#{value}) not in domain: #{option[:domain]}."
         exit(4)
      end

      optionValues[currentOption[:id]] = value
   end

   return optionValues
end

def main(args)
   genData(loadArgs(args))
end

if ($0 == __FILE__)
   main(ARGV)
end

# Calculate an upper bound on the number of groundings (assuming equal block division).
# Note that the actual number will be reduced is the number of people is not
# divisible by te number of locations or if there are some 0 similarity scores.

module ComputeNumberGroundins
   def ComputeNumberGroundins.permutation(n, r)
      return ComputeNumberGroundins.factorial(n) / ComputeNumberGroundins.factorial(n - r)
   end

   def ComputeNumberGroundins.factorial(n)
      return (1..n).reduce(1, :*)
   end

   def ComputeNumberGroundins.numberGroundings(people, locations)
      # Upper bound if not evenly disivible.
      blockSize = (people.to_f() / locations).ceil()

      return 3 * (ComputeNumberGroundins.permutation(blockSize, 2) * locations) + (ComputeNumberGroundins.permutation(blockSize, 3) * locations)
   end

   def ComputeNumberGroundins.main(args)
      startPeople = 200
      endPeople = 600
      peopleIncrement = 100

      startLocations = 10
      endLocations = 30
      locationsIncrement = 10

      people = startPeople
      while (people <= endPeople)
         locations = startLocations
         while (locations <= endLocations)
            puts "People: #{people}, Locations: #{locations}, Num Groundings: #{numberGroundings(people, locations)}"

            locations += locationsIncrement
         end

         people += peopleIncrement
      end
   end
end

if ($0 == __FILE__)
   ComputeNumberGroundins.main(ARGV)
end

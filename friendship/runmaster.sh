mvn compile
java -cp ./target/classes:`cat classpath.out` org.linqs.psl.example.Friendship --master

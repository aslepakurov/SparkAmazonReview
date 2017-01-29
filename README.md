# Spark Amazon Review


Spark Amazon Review analysis project.

To run this project:

```
1) git clone https://github.com/aslepakurov/SparkAmazonReview.git
2) cd SparkAmazonReview
3) mvn clean package
4) java -cp "target/spark-amazon-review-1.0-SNAPSHOT-jar-with-dependencies.jar:target/jars/*" com.aslepakurov.spark.SparkLocalRunner /home/barabashka/Downloads/amazon-fine-foods/Reviews.csv /home/barabashka/Downloads/amazon-fine-foods/out
```

Spark is easy scalable and highly configurable (resource-wise)

Files used for analysis can be downloaded here(sign up required):

```
https://www.kaggle.com/snap/amazon-fine-food-reviews
```
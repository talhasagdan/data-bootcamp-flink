# Trendyol Data Bootcamp Apache Flink Course Material

This repo contains an example and a homework used in the Apache Flink course for Trendyol Data Bootcamp.

## Purpose of Homework:

### Likelihood to Purchase Calculator Homework

In this homework, you will calculate likelihood to purchase ratios based on user and products.

A coefficient map is provided in `LikelihoodToPurchaseCalculator` source file as `l2pCoefficients`. Note that four event types has no effect on purchase, which are `CompletePurchase`, `Logout`, `Login`, `DisplayOrders`.

### Expectations
* Use RandomEventSource as source operator,
* Use PrintSinkFunction as sink operator (This is not mandatory, you can choose some other sink if you want to try one),
* Use 20 seconds for time window interval,
* Use processing time as event time triggers lots of records before window completes (This is not mandatory, you can also use event time but it helps you on reasoning about results you see).



#### Project created for Kodluyoruz Trendyol Data Talent Bootcamp

<img src="https://github.com/talhasagdan/kodluyoruz-scala/blob/main/scala-flist-streetfinder/src/img/1_posh7DaGCQA8Ku-qkxrdyQ.jpeg?raw=true" width=150/> <img src="https://github.com/talhasagdan/kodluyoruz-scala/blob/main/scala-flist-streetfinder/src/img/30476529.png?raw=true" width=150/>
